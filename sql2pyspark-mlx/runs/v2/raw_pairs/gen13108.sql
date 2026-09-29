SELECT merchant, SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) 7_eleven_count, COUNT(amount) amount_count FROM transactions GROUP BY merchant ORDER BY amount_count
