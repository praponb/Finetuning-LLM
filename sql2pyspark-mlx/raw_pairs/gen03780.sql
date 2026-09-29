WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM web_events
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY prev_event_time DESC NULLS LAST, id) AS rank_in_group
    FROM sessions_web_events
    QUALIFY rank_in_group = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessions_web_events_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_3
    FROM flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        page,
        session_id_2,
        rank_in_group,
        is_new_burst,
        session_id_3
    FROM sessions_web_events_2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        page,
        rank_in_group,
        is_new_burst,
        session_id_3,
        SUM(session_id_2) OVER (PARTITION BY session_id ORDER BY session_id_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_id_2,
        LAG(session_id_3, 1) OVER (PARTITION BY page ORDER BY rank_in_group, id) AS prev_session_id_3
    FROM latest
    WHERE page <> '/product'
),
latest_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY prev_session_id_3 NULLS LAST, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        is_new_burst,
        cumulative_session_id_2,
        prev_session_id_3,
        row_num,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_new_burst,
        LAG(cumulative_session_id_2, 1, 0) OVER (ORDER BY row_num, id) AS prev_cumulative_session_id_2
    FROM latest_web_events
)
SELECT
    id,
    page,
    cumulative_session_id_2,
    prev_session_id_3,
    prev_cumulative_session_id_2
FROM scored;
