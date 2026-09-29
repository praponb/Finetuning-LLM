WITH with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
)
SELECT
    id,
    order_id,
    method,
    paid_at,
    SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM with_gaps
