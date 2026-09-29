WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
)
-- Running sum of the flag = session number
SELECT
    id,
    order_id,
    method,
    paid_at,
    status,
    SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
FROM gaps;
