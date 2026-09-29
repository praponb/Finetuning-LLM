SELECT
    orders.id,
    orders.channel,
    orders.customer_id,
    t.min_cost,
    t.unique_statuss
FROM orders
LEFT JOIN (SELECT order_id, MIN(cost) AS min_cost, COUNT(DISTINCT cost) AS unique_statuss
    FROM shipments
    GROUP BY order_id) t ON orders.id = t.order_id
WHERE t.unique_statuss >= 5
