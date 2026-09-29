WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        paid_at,
        LENGTH(method) AS method_length
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-10-07 03:00:00'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    CASE WHEN method_length >= 86 THEN 'LARGE' ELSE 'SMALL' END AS method_length_tier,
    CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    SUM(CASE WHEN method = 'card' THEN burst_id ELSE 0 END) AS card_burst_id,
    COUNT(*) AS num_rows,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min
FROM sessions_payments
GROUP BY GROUPING SETS ((CASE WHEN method_length >= 86 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN burst_id >= 7 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN method_length >= 86 THEN 'LARGE' ELSE 'SMALL' END), ());
