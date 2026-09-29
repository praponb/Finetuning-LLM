SELECT store_id, SUM(amount) AS total_amount, MAX(order_date) AS last_order_date
FROM orders
WHERE status IN ('SHIPPED', 'CANCELLED', 'PENDING') AND channel NOT IN ('partner', 'store')
GROUP BY store_id
HAVING SUM(amount) > 7200;
