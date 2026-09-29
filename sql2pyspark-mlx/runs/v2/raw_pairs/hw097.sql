SELECT region, city, SUM(sqft) AS total_sqft FROM stores GROUP BY ROLLUP(region, city);
