SELECT status, COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count, MIN(order_date) AS min_order_date
FROM orders
GROUP BY status
