SELECT city, region, ROUND(AVG(sqft), 2) AS avg_sqft, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai FROM stores GROUP BY city, region;
