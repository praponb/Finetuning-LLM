WITH order_totals AS (
    SELECT order_id, MIN(cost) min_cost
    FROM shipments
    WHERE order_id IS NULL
    GROUP BY order_id
)
SELECT o.id, o.order_date, ot.min_cost
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id;
