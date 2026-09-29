WITH enriched AS (
    SELECT
        id,
        event_type,
        duration_sec,
        MIN(duration_sec) OVER (PARTITION BY event_type) AS event_type_min_duration_sec,
        CUME_DIST() OVER (ORDER BY duration_sec ASC NULLS FIRST) AS duration_sec_cume_dist,
        SUM(duration_sec) OVER (PARTITION BY page, session_id ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_sum_30d
    FROM web_events
)
SELECT
    id,
    event_type,
    duration_sec,
    event_type_min_duration_sec,
    duration_sec_cume_dist,
    CASE WHEN event_type RLIKE '^(page_view|click)$' THEN 'PRIORITY' WHEN event_type RLIKE '^(add_to_cart|purchase)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS event_type_zone
FROM enriched
ORDER BY event_type ASC, event_type_min_duration_sec ASC
