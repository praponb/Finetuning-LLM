WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    session_id,
    COUNT(*) AS cnt,
    ROUND(SUM(new_group_flag) / NULLIF(COUNT(CASE WHEN page = '/checkout' THEN 1 END), 0), 2) AS new_group_flag_per_checkout,
    GROUPING_ID() AS grouping_id
FROM sessions_web_events
GROUP BY ROLLUP (session_id)
ORDER BY session_id DESC NULLS LAST;
