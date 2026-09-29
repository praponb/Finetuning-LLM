SELECT MAX(amount) AS max_amount
FROM orders
WHERE MONTH(order_date) IN (7, 8, 9) AND status <> 'RETURNED'
ORDER BY max_amount DESC;
