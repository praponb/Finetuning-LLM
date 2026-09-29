WITH OrderSummary AS (
    SELECT order_id, ROUND(AVG(cost), 2) AS avg_cost, MIN(shipped_date) AS first_shipped_date
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.channel,
    o.amount,
    O.avg_cost,
    O.first_shipped_date
FROM orders o
INNER JOIN OrderSummary O ON o.id = O.order_id
ORDER BY 4 DESC, first_shipped_date;
