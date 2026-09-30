WITH base AS (
    SELECT *, DATE_TRUNC('week', shipped_date) AS shipped_date_period
    FROM shipments
    WHERE order_id IN (8, 25) AND order_id IN (23, 25, 26) AND status IN ('LOST', 'RETURNED', 'IN_TRANSIT')
),
shipments_stats AS (
    SELECT status, MIN(cost) AS min_cost
    FROM base
    GROUP BY status
)
SELECT status, min_cost
FROM shipments_stats
WHERE min_cost > 27
ORDER BY status ASC, min_cost DESC;
