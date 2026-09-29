WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN amount >= 1390 THEN 'High' WHEN amount >= 280 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM payments
    WHERE order_id IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
-- Grouped metrics
SELECT
    order_id,
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    AVG(amount) FILTER (WHERE method = 'paypal') AS paypal_avg_amount,
    SUM(CASE WHEN status = 'FAILED' THEN amount ELSE 0 END) AS failed_amount,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count
FROM sessions_payments
GROUP BY 1, 2
