WITH base AS (
    SELECT *
    FROM shipments
    WHERE carrier IN ('DHL', 'Flash', 'UPS')
),
summary AS (
    SELECT order_id, COUNT(*) AS cnt, MAX(cost) - MIN(cost) AS cost_range
    FROM base
    GROUP BY order_id
)
SELECT order_id, cnt, cost_range
FROM summary
ORDER BY order_id DESC;
