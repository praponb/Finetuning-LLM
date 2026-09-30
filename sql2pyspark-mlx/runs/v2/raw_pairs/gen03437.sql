SELECT city, sqft_count FROM (SELECT city, COUNT(sqft) AS sqft_count FROM stores GROUP BY city) AS x WHERE sqft_count > 9 ORDER BY sqft_count, city LIMIT 3
