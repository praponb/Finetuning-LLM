WITH src AS (
    SELECT *
    FROM shipments
    WHERE status != 'LOST' AND shipped_date < '2024-07-23'
),
summary AS (
    SELECT order_id, SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) AS delivered_cost
    FROM src
    GROUP BY order_id
)
SELECT order_id, delivered_cost
FROM summary
WHERE delivered_cost > 49
ORDER BY order_id DESC, delivered_cost DESC
LIMIT 5;
