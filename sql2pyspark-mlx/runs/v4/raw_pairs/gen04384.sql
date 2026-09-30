SELECT region, ROUND(STDDEV(sqft), 2) AS stddev_sqft FROM stores GROUP BY region ORDER BY stddev_sqft DESC
