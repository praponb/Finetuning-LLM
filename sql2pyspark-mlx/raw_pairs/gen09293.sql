WITH with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    method,
    amount,
    status,
    prev_paid_at,
    session_start_flag,
    island_id,
    CASE WHEN status RLIKE '^(FAILED|SUCCESS)$' THEN 'EXTENDED' ELSE 'CORE' END AS status_zone
FROM sessions_payments
ORDER BY amount DESC NULLS FIRST;
