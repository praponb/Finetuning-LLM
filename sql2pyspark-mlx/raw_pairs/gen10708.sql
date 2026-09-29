WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN amount >= 1250 THEN 'High' WHEN amount >= 760 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at >= DATE '2023-04-20'
      AND method IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        amount_tier,
        method_filled,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    order_id,
    method,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(method)) AS num_methods,
    SUM(CASE WHEN method = 'paypal' THEN amount ELSE 0 END) AS paypal_amount,
    SUM(CASE WHEN method = 'cash' THEN burst_id ELSE 0 END) AS cash_burst_id
FROM sessions_payments
GROUP BY DATE_TRUNC('WEEK', paid_at), order_id, method;
