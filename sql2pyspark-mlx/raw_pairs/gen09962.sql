SELECT city, COUNT(*) AS total_count, COUNT(*) AS store_count FROM stores GROUP BY city HAVING MAX(sqft) > 11500
