SELECT
    orders.amount,
    orders.customer_id,
    t.unique_statuss,
    t.returned_count
FROM orders
LEFT OUTER JOIN (SELECT order_id, COUNT(DISTINCT cost) AS unique_statuss, SUM(CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count
    FROM shipments
    GROUP BY order_id) t ON orders.id = t.order_id
