WITH with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
    CASE WHEN duration_sec >= 409 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT_IF(event_type = 'add_to_cart') AS add_to_cart_events,
    SUM(CASE WHEN event_type = 'click' THEN session_no ELSE 0 END) AS click_session_no
FROM sessions_web_events
GROUP BY CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN duration_sec >= 409 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY add_to_cart_events ASC
