WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS dedup_rank
        FROM web_events
    ) ranked
    WHERE dedup_rank = 1
),
ranked_web_events AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        event_time,
        duration_sec
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY dedup_rank, id) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM ranked_web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_web_events
),
sessions_web_events_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_time,
        src.duration_sec,
        p.browser
    FROM sessions_web_events_2 src
    LEFT JOIN sessions p
        ON src.session_id = p.id
    WHERE HOUR(p.started_at) BETWEEN 9 AND 16
)
SELECT
    s.id,
    s.session_id,
    s.event_time,
    s.duration_sec,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM with_dims s
ORDER BY sum_web_event_duration_sec NULLS LAST;
