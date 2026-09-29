WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM sessions
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
)
-- Running sum of the flag = session number
SELECT
    id,
    customer_id,
    started_at,
    country,
    SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
FROM gaps;
