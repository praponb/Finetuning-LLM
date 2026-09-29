WITH not_flagged AS (
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
)
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    GROUPING_ID() AS grouping_id
FROM not_flagged
GROUP BY CUBE (DATE_TRUNC('YEAR', started_at), DATE_TRUNC('QUARTER', started_at))
ORDER BY quarter_start;
