WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost, id) AS rn
    FROM shipments
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
    ROUND(AVG(cost), 2) AS avg_cost,
    RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', delivered_date) ORDER BY APPROX_PERCENTILE(cost, 0.95) DESC) AS cost_p95_rank
FROM deduped
GROUP BY DATE_TRUNC('QUARTER', delivered_date), DATE_TRUNC('MONTH', delivered_date);
