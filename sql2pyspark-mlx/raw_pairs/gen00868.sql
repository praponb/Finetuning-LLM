WITH flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_session_tier,
    session_id,
    SUM(CASE WHEN page = '/product' THEN is_new_session ELSE 0 END) AS product_is_new_session,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    SIZE(COLLECT_SET(page)) AS num_pages,
    GROUPING_ID() AS grouping_level
FROM islands
GROUP BY GROUPING SETS ((CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END, session_id), (CASE WHEN is_new_session >= 1 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING product_is_new_session > 0
ORDER BY product_is_new_session DESC;
