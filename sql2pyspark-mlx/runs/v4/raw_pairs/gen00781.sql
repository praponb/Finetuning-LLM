WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUBSTRING(status, 1, 3) AS status_prefix
    FROM payments
    WHERE order_id IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    CASE WHEN island_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS island_id_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    APPROX_PERCENTILE(amount, 0.9) AS amount_p90,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    MAX(island_id) FILTER (WHERE status = 'FAILED') AS failed_max_island_id,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min_2,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM sessions_payments
GROUP BY DATE_TRUNC('QUARTER', paid_at), CASE WHEN island_id >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', paid_at)
HAVING duration_min >= 0
