WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
)
-- Running sum of the flag = session number
SELECT
    id,
    customer_id,
    device,
    started_at,
    SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
FROM with_gaps
