SELECT ROUND((100 * SUM(CASE WHEN merchant = 'Amazon' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_amazon FROM transactions
