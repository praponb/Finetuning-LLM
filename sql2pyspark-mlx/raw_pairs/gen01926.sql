WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    burst_id,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    SIZE(COLLECT_SET(method)) AS num_methods,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    PERCENT_RANK() OVER (PARTITION BY burst_id ORDER BY STDDEV_SAMP(amount) DESC) AS amount_stddev_percent_rank
FROM sessions_payments
GROUP BY burst_id, DATE_TRUNC('MONTH', paid_at)
