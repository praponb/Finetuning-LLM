WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CONCAT_WS('-', method, status) AS method_status_key
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-10-27 20:00:00'
      AND order_id IS NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    CASE WHEN amount >= 1210 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    ROUND(SUM(burst_id) / NULLIF(COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END), 0), 2) AS burst_id_per_bank_transfer,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    MIN(paid_at) AS first_paid_at,
    MAX(paid_at) AS last_paid_at
FROM sessionized
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', paid_at), CASE WHEN amount >= 1210 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('YEAR', paid_at)), ())
