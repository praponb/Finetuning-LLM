SELECT status, store_id, SUM(discount) AS total_discount, COUNT(DISTINCT discount) AS unique_statuss FROM orders GROUP BY CUBE(status, store_id) ORDER BY store_id DESC;
