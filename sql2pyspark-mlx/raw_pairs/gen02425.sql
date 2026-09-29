SELECT city, AVG(sqft) AS avg_sqft, COUNT(*) AS row_count FROM stores GROUP BY city
