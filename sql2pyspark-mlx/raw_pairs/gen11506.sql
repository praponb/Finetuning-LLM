SELECT region, min_sqft FROM (SELECT region, MIN(sqft) AS min_sqft FROM stores GROUP BY region) AS x WHERE min_sqft < 15300 ORDER BY min_sqft DESC;
