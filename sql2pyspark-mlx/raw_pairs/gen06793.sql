WITH OrderSummary AS (
    SELECT order_id, MAX(cost) AS max_cost
    FROM shipments
    GROUP BY order_id
)
SELECT o.id, o.discount, O.max_cost
FROM orders o
LEFT JOIN OrderSummary O ON o.id = O.order_id
WHERE O.max_cost >= 69
