WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        CASE WHEN page = '/home' THEN 1 ELSE 0 END AS is_home
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-05-22 14:00:00'
      AND page IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec) AS dedup_rank
        FROM base
    ) x
    WHERE dedup_rank = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM ranked
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_event_time) OVER (PARTITION BY session_id ORDER BY prev_event_time, id) IS NULL
                 OR prev_event_time > LAG(prev_event_time) OVER (PARTITION BY session_id ORDER BY prev_event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst_2
    FROM sessionized
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        dedup_rank,
        prev_event_time,
        is_new_burst,
        island_id,
        SUM(is_new_burst_2) OVER (PARTITION BY session_id ORDER BY prev_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.dedup_rank,
        s.island_id,
        p.country,
        p.browser
    FROM sessions_web_events s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.customer_id IS NOT NULL
)
SELECT
    id,
    dedup_rank,
    country,
    browser
FROM enriched
ORDER BY browser DESC;
