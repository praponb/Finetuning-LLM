WITH prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    DATE_TRUNC('YEAR', prev_paid_at) AS year_start,
    MAX(new_group_flag) FILTER (WHERE method = 'card') AS card_max_new_group_flag,
    MIN(burst_id) AS min_burst_id
FROM sessionized
GROUP BY DATE_TRUNC('YEAR', prev_paid_at)
