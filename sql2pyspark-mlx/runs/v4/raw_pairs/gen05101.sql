WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        MAX(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) duration_sec_max_30d,
        LEAD(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY duration_sec, id) next_duration_sec
    FROM web_events
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_count_7d,
        MAX(duration_sec_max_30d) OVER w_hist duration_sec_max_30d_max_90d
    FROM latest
    WINDOW w_hist AS (PARTITION BY event_type, page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM scored
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    event_type,
    CASE WHEN duration_sec_max_30d >= 437 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_max_30d_tier,
    CASE WHEN duration_sec_count_7d >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_count_7d_tier,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    COUNT_IF(event_type = 'page_view') AS page_view_events,
    COUNT(*) AS cnt,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN page = '/home' THEN 1 END), 0), 2) AS is_new_burst_per_home
FROM sessionized
GROUP BY event_type, CASE WHEN duration_sec_max_30d >= 437 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN duration_sec_count_7d >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY duration_sec_max_30d_tier;
