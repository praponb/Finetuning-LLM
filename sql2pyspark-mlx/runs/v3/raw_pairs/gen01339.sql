SELECT *
FROM (
    SELECT customer_id, COUNT(*) AS num_orders
    FROM orders
    WHERE order_date <= '2024-11-01'
    GROUP BY customer_id
) AS x
WHERE num_orders > 4
ORDER BY num_orders
