SELECT channel, MAX(order_date) AS last_order_date
FROM orders
WHERE status = 'SHIPPED' AND (store_id IN (15, 18, 22, 26) OR store_id = 36)
GROUP BY 1
ORDER BY channel, last_order_date
