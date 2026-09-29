WITH order_totals AS (
    SELECT order_id, MAX(cost) - MIN(cost) AS cost_range
    FROM shipments
    WHERE delivered_date BETWEEN '2024-01-01' AND '2025-08-07'
    GROUP BY order_id
)
SELECT o.channel, o.store_id, ot.cost_range
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id;
