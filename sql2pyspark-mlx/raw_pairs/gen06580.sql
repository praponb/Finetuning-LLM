WITH valid AS (
    SELECT *, CASE WHEN page IN ('/product', '/checkout') THEN 'Tier 1' ELSE 'Other' END page_group
    FROM web_events
    WHERE duration_sec <= 214
),
gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessions_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_windows AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        page_group,
        new_group_flag,
        burst_id,
        AVG(duration_sec) OVER (PARTITION BY page, session_id ORDER BY new_group_flag ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_duration_sec,
        COUNT(duration_sec) OVER (PARTITION BY page, session_id ORDER BY new_group_flag ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY page, session_id ORDER BY new_group_flag ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_max_duration_sec
    FROM sessions_web_events
),
scored AS (
    SELECT
        id,
        event_type,
        event_time,
        last7_avg_duration_sec,
        last7_max_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY page, event_type ORDER BY last7_max_duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(event_time) ORDER BY last7_avg_duration_sec ASC, id ASC) event_seq,
        LAG(last7_avg_duration_sec, 1) OVER (PARTITION BY page ORDER BY last7_count_duration_sec ASC, id ASC) prev_last7_avg_duration_sec,
        COUNT(burst_id) OVER (PARTITION BY YEAR(event_time), event_type ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) burst_id_count_90d,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY last7_count_duration_sec ASC, id ASC) row_seq
    FROM with_windows
    WHERE last7_avg_duration_sec < 480
)
SELECT
    id,
    event_type,
    event_time,
    last7_avg_duration_sec,
    last7_max_duration_sec,
    last_known_duration_sec,
    burst_id_count_90d,
    row_seq,
    CASE WHEN event_type RLIKE '^(page_view|purchase)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS event_type_zone
FROM scored
ORDER BY event_type_zone ASC;
