SELECT store_id, COUNT(*) AS total_count FROM store_sales GROUP BY store_id ORDER BY total_count DESC
