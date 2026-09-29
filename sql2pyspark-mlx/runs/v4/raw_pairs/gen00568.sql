WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(event_type, ', ', page) event_type_page,
        COALESCE(page, 'NONE') resolved_page
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-08-18 00:00:00')
      AND page IS NOT NULL
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM filtered
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_type_page,
        resolved_page,
        prev_event_time,
        new_group_flag,
        session_id_2,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_sum_7d,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY prev_event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) duration_sec_avg_6,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY session_id_2, id) row_seq
    FROM sessionized
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY duration_sec_sum_7d NULLS FIRST, id) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        page,
        duration_sec_sum_7d
    FROM ranked
) src
PIVOT (
    AVG(duration_sec_sum_7d) AS total,
    COUNT(1) AS cnt
    FOR page IN ('/home' AS home, '/search' AS search)
);
