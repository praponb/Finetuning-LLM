WITH order_summary AS (
    SELECT order_id, MAX(cost) AS max_cost
    FROM shipments
    WHERE shipped_date BETWEEN '2023-04-07' AND '2024-08-01'
    GROUP BY order_id
)
SELECT o.customer_id, o.amount, os.max_cost
FROM orders o
JOIN order_summary os ON o.id = os.order_id
ORDER BY customer_id DESC, amount;
