/* 
This is a Java skeleton code to help you out with how to start this assignment.
Please remember that this is NOT a compilable/runnable java file.
Please feel free to use this skeleton code.
Please look closely at the "To Do" parts of this file. You may get an idea of how to finish this assignment. 
*/

import java.util.*;

import com.mysql.cj.x.protobuf.MysqlxConnection.Close;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.PrintWriter;

class Assign2Skeleton {
   
   static class StockData {	   
	   // To Do: 
	   // Create this class which should contain the information  (date, open price, high price, low price, close price) for a particular ticker
      //need a getter and setter to assign variables (this is my comment)
      String Ticker;
      String TransDate;
      Double OpenPrice;
      Double HighPrice;
      double LowPrice;
      double ClosePrice;
      double Volume;
      double Adjusted;

      public void setData(String TransDate, Double OpenPrice, double HighPrice, double LowPrice, double ClosePrice, double Volume, double Adjusted) {
         this.TransDate = TransDate;
         this.OpenPrice = OpenPrice;
         this.HighPrice = HighPrice;
         this.LowPrice = LowPrice;
         this.ClosePrice = ClosePrice;
         this.Volume = Volume;
         this.Adjusted = Adjusted;
      }

      public double getOpen() {
         return this.OpenPrice;
      }

      public double getClose() {
         return this.ClosePrice;
      }
   }
   
   static Connection conn;
   static final String prompt = "Enter ticker symbol [start/end dates]: ";
   
   public static void main(String[] args) throws Exception {
	  String paramsFile = "ConnectionParameters_LabComputer.txt";
	  //String paramsFile = "ConnectionParameters_RemoteComputer.txt";

      if (args.length >= 1) {
         paramsFile = args[0];
      }
      
      Properties connectprops = new Properties();
      connectprops.load(new FileInputStream(paramsFile));
      try {
         Class.forName("com.mysql.cj.jdbc.Driver");
         String dburl = connectprops.getProperty("dburl");
         String username = connectprops.getProperty("user");
         conn = DriverManager.getConnection(dburl, connectprops);
         System.out.println("Database connection is established");
         
         Scanner in = new Scanner(System.in);
         System.out.print(prompt);
         String input = in.nextLine().trim();
         
         while (input.length() > 0) {
            String[] params = input.split("\\s+");
            String ticker = params[0];
            String startdate = null, enddate = null;
            if (getName(ticker)) {
               if (params.length >= 3) {
                  startdate = params[1];
                  enddate = params[2];
               }               
               Deque<StockData> data = getStockData(ticker, startdate, enddate);
               System.out.println();
               System.out.println("Executing investment strategy");
               doStrategy(ticker, data);
            } 
            
            System.out.println();
            System.out.print(prompt);
            input = in.nextLine().trim();
         }

         // Close the database connection

      } catch (SQLException ex) {
         System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                           ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
      }
   }
   
   static boolean getName(String ticker) throws SQLException {
	  // To Do: 
	  // Execute the first query and print the company name of the ticker user provided (e.g., INTC to Intel Corp.) 
	  // Please don't forget to use a prepared statement
     boolean exists = false;
     PreparedStatement pstmt = conn.prepareStatement(
                           "select Name from company where Ticker = ?");

      pstmt.setString(1, ticker);
      ResultSet rs = pstmt.executeQuery();

      if (rs.next()) {
         exists = true;
         System.out.printf("%s%n", rs.getString("Name"));
      } else {
         System.out.printf("Ticker %s not in database%n", ticker);
      }
      pstmt.close();
      return exists;
   }

   static Deque<StockData> getStockData(String ticker, String start, String end) throws SQLException {	  
	  // To Do: 
	  // Execute the second query, which will return stock information of the ticker (descending on the transaction date)
	  // Please don't forget to use a prepared statement
      PreparedStatement pstmt;
      Deque<StockData> result = new ArrayDeque<>();
     
      // if only ticker given
      if (start == null || end == null) {
         // Prepare query
         pstmt = conn.prepareStatement(
            "select * " +
            "  from pricevolume " +
            "  where Ticker = ?" +
            "  order by TransDate DESC");
         // Fill in the blanks
         pstmt.setString(1, ticker);
      }  else {
         // Prepare query
         pstmt = conn.prepareStatement(
            "select * " +
            "  from pricevolume " +
            "  where Ticker = ? and TransDate between ? and ?" +
            "  order by TransDate DESC");
         // Fill in the blanks
         pstmt.setString(1, ticker);
         pstmt.setString(2, start);
         pstmt.setString(3, end);
      }
      ResultSet rs = pstmt.executeQuery();
      double tomorrowsOpen = 0.0;
      double splitMultiplier = 1;
      int tradingDays = 0;
      int numSplits = 0;

      // To Do: 
	   // Loop through all the dates of that company (descending order)
		// Find a split if there is any (2:1, 3:1, 3:2) and adjust the split accordingly
      while (rs.next()) {
         String transDate = rs.getString("TransDate").trim();
         double openPrice = Double.parseDouble(rs.getString("OpenPrice").trim());
         double highPrice = Double.parseDouble(rs.getString("HighPrice").trim());
         double lowPrice = Double.parseDouble(rs.getString("LowPrice").trim());
         double closePrice = Double.parseDouble(rs.getString("ClosePrice").trim());
         double volume = Double.parseDouble(rs.getString("Volume").trim());
         double adjustedClose = Double.parseDouble(rs.getString("AdjustedClose").trim());

         tradingDays++;


         //Check 3:2 split
         if ((Math.abs(closePrice / tomorrowsOpen - 1.5) < 0.15) && tomorrowsOpen != 0.0) {
            System.out.printf("3:2 split on " + transDate + "\t" + closePrice + " --->    " + tomorrowsOpen + "\n");
            splitMultiplier = splitMultiplier * 1.5;
            numSplits++;
         }

         //Check 2:1 split
         if ((Math.abs(closePrice / tomorrowsOpen - 2.0) < 0.20) && tomorrowsOpen != 0.0) {
            System.out.printf("2:1 split on " + transDate + "\t" + closePrice + " --->    " + tomorrowsOpen + "\n");
            splitMultiplier = splitMultiplier * 2;
            numSplits++;
         }

         //Check 3:1 split
         if ((Math.abs(closePrice / tomorrowsOpen - 3.0) < 0.30) && tomorrowsOpen != 0.0) {
            System.out.printf("3:1 split on " + transDate + "\t" + closePrice + " --->    " + tomorrowsOpen + "\n");
            splitMultiplier = splitMultiplier * 3;
            numSplits++;
         }
         
         
         //Create StockData object and initialize with adjusted open, high, low, and close
         StockData row = new StockData();
         row.setData(transDate,
                     openPrice / splitMultiplier, 
                     highPrice / splitMultiplier,
                     lowPrice / splitMultiplier,
                     closePrice / splitMultiplier,
                     volume,
                     adjustedClose / splitMultiplier
                     );
         

         /*
         System.out.printf("Ticker: %s, TransDate: %s, Open: %.2f, High: %.2f, Low: %.2f, Close: %.2f%n, Volume: %.2f%n, AdjustedClose: %.2f%n",
         */

         //add adjusted data to Deque
         result.addFirst(row);

         //Save opening price for comparison in next loop
         tomorrowsOpen = openPrice;
      }
      System.out.printf(numSplits + " splits in " + tradingDays + " trading days\n");

      pstmt.close();

      return result;
   }
   
   static void doStrategy(String ticker, Deque<StockData> data) {
	   //To Do: 
	   // Apply Steps 2.6 to 2.10 explained in the assignment description 
	   // data (which is a Deque) has all the information (after the split adjustment) you need to apply these steps
      double netGain = 0.0;
      int shares = 0;
      int transactions = 0;
      double fiftyDayAvg = 0.0;

      Iterator<StockData> itr = data.descendingIterator();
      ArrayList<Double> fiftyDayAvgs = new ArrayList<Double>();
      ArrayList<Double> openPrice = new ArrayList<Double>();
      ArrayList<Double> closePrice = new ArrayList<Double>();

      //Populate open and close array lists
      while (itr.hasNext()) {
         StockData today = itr.next();
         openPrice.add(today.getOpen());
         closePrice.add(today.getClose());
      }

      
      //Less than 50 days of data available
      if(closePrice.size() < 51) {
         System.out.printf("No trading, net gain of $0\n");
      } else {
         int i = 1;
         int j = 0;
         while ((51+j) < closePrice.size()) {
            while ((i+j) < (51+j)) {
               fiftyDayAvg += closePrice.get(i);
               i++;
            }

            //add 50 day avg to array list, then continue to 50 day avg of d - 1
            fiftyDayAvgs.add(fiftyDayAvg/50);
            j++;
         }
      
      
         
         boolean boughtShares;
         for (i = fiftyDayAvgs.size() - 1; i >= 1; i--) {
            boughtShares = false;

            //Buy Criterion
            if (closePrice.get(i) < fiftyDayAvgs.get(i) && (closePrice.get(i)/openPrice.get(i) <= 0.97)) {
               boughtShares = true;
               shares += 100;
               netGain -= openPrice.get(i-1) - 8;
               transactions++;
            }

            //Sell Criterion
            if (!boughtShares && shares >= 100 && (openPrice.get(i) > fiftyDayAvgs.get(i)) && (openPrice.get(i)/closePrice.get(i+1) >= 1.01)) {
               shares -= 100;
               netGain += (openPrice.get(i) + closePrice.get(i)/2) - 8;
               transactions++;
            }
      
         }

         //Sell remaining shares
         if (shares > 0) {
            netGain = shares * openPrice.get(0);
            shares = 0;
            transactions++;
         }

         System.out.printf("Transactions executed: " + transactions + "\nNet Cash: " + netGain + "\n");
      }
      
   }
}