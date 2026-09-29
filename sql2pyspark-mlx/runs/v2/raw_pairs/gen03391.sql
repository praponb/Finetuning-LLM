SELECT SUM(CASE WHEN merchant = '7-Eleven' THEN amount ELSE 0 END) AS 7_eleven_amount FROM transactions;
