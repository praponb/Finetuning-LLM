SELECT region, sqft_range, min_sqft FROM (SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range, MIN(sqft) AS min_sqft FROM stores GROUP BY region) AS t WHERE min_sqft >= 8600 ORDER BY region DESC;
