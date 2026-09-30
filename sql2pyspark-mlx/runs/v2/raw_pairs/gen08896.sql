SELECT c.is_active, MAX(o.order_date) AS last_order_date, COUNT(*) AS order_count
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY c.is_active;
