SELECT region, ROUND(STDDEV(sqft), 2) stddev_sqft FROM stores GROUP BY 1 HAVING AVG(sqft) > 7600 ORDER BY stddev_sqft DESC, region DESC
