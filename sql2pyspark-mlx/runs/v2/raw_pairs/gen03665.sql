SELECT SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END) AS east_count, ROUND(STDDEV(sqft), 2) AS stddev_sqft, MIN(sqft) AS min_sqft FROM stores ORDER BY stddev_sqft;
