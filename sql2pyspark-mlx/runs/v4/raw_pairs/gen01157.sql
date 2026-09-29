WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
)
-- Running sum of the flag = session number
SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM gaps;
