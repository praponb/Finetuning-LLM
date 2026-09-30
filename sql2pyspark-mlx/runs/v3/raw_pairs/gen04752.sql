SELECT COUNT(*) AS total_count, SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count
FROM payments
WHERE order_id = 30
