SELECT COUNT(*) AS total_count FROM orders WHERE store_id IN (3, 12, 22, 30) AND store_id IS NOT NULL ORDER BY total_count DESC;
