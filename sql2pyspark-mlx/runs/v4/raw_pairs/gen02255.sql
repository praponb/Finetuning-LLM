WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost, 1) OVER (ORDER BY cost, id) AS prev_cost,
        LAST_VALUE(cost, TRUE) OVER w_recent AS last_known_cost
    FROM shipments
    WINDOW w_recent AS (PARTITION BY order_id, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    carrier,
    SUM(CASE WHEN carrier = 'Kerry' THEN last_known_cost ELSE 0 END) AS kerry_last_known_cost,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    AVG(cost) AS avg_cost,
    APPROX_PERCENTILE(prev_cost, 0.95) AS prev_cost_p95,
    COUNT(*) FILTER (WHERE status = 'DELIVERED') AS delivered_rows
FROM scored
GROUP BY carrier
HAVING kerry_last_known_cost > 0
