SELECT city, ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_th FROM customers GROUP BY city ORDER BY pct_th
