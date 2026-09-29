SELECT city, max_sqft FROM (SELECT city, MAX(sqft) AS max_sqft FROM stores WHERE opened_date < '2023-06-01' GROUP BY city) AS x WHERE max_sqft > 7600
