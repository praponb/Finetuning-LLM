WITH base AS (
    SELECT
        *,
        LENGTH(page) AS page_length,
        CONCAT(page, ' ', event_type) AS page_event_type,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE event_time > TO_DATE('2023-10-23')
),
ranked AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type, session_id ORDER BY event_time ASC, id ASC) = 1
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC NULLS FIRST, id ASC) AS rn
    FROM ranked
    QUALIFY rn = 1
),
metrics AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        page_event_type,
        resolved_page,
        SUM(duration_sec) OVER (PARTITION BY event_type, page ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_max_7d,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY rn ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6
    FROM deduped
    WHERE session_id = 7
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_gaps_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM islands
),
islands_web_events AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps_2
)
SELECT
    id,
    event_type,
    event_time,
    duration_sec,
    cumulative_duration_sec,
    duration_sec_avg_6,
    is_new_session,
    session_id,
    new_group_flag
FROM islands_web_events
ORDER BY duration_sec_avg_6 DESC NULLS LAST;
