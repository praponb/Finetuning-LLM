WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(method, '/', status) AS method_status
    FROM payments
    WHERE method IS NOT NULL
      AND paid_at >= TIMESTAMP '2023-05-16 18:00:00'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    ROUND(AVG(session_id), 2) AS avg_session_id,
    SUM(CASE WHEN method = 'promptpay' THEN is_new_session ELSE 0 END) AS promptpay_is_new_session,
    COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    RANK() OVER (PARTITION BY CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN method = 'promptpay' THEN is_new_session ELSE 0 END) DESC) AS promptpay_is_new_session_rank
FROM sessions_payments
GROUP BY CASE WHEN amount >= 1490 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', paid_at)
HAVING COUNT(*) FILTER (WHERE method = 'bank_transfer') >= 1
