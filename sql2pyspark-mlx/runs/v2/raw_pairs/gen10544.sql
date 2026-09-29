SELECT status, MIN(order_date) AS min_order_date, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
FROM orders
GROUP BY status
ORDER BY returned_count DESC, status ASC
LIMIT 10
