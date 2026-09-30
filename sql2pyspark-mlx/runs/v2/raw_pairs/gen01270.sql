SELECT * FROM (SELECT city, ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin FROM stores GROUP BY city) AS agg WHERE pct_berlin < 80;
