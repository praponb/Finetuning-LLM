WITH base AS (
    SELECT *
    FROM shipments
    WHERE order_id IS NOT NULL
),
shipments_stats AS (
    SELECT status, COUNT(*) AS total_count
    FROM base
    GROUP BY status
)
SELECT *
FROM shipments_stats
WHERE total_count < 10
ORDER BY status DESC
