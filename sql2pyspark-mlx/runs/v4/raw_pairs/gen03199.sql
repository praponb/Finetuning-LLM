WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    COUNT(CASE WHEN status = 'REFUNDED' THEN 1 END) AS refunded_count,
    MAX(island_id) FILTER (WHERE method = 'paypal') AS paypal_max_island_id,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount,
    MIN(paid_at) AS started_at,
    SUM(amount) FILTER (WHERE status = 'FAILED') AS failed_total_amount
FROM islands
GROUP BY CUBE (DATE_TRUNC('WEEK', paid_at), DATE_TRUNC('YEAR', paid_at))
