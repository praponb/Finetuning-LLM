WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
)
-- Running sum of the flag = session number
SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM gaps
