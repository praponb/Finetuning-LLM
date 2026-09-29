WITH gaps AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    country,
    SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
FROM gaps
