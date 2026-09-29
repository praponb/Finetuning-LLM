SELECT city, ROUND((100 * SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_th FROM suppliers GROUP BY city;
