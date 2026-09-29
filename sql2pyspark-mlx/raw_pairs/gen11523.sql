WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(page) AS page_upper,
        COALESCE(page, 'NONE') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY YEAR(event_time) ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page_2
    FROM sessionized
),
joined AS (
    -- Attach sessions attributes
    SELECT e.*, b.device
    FROM scored e
    LEFT JOIN sessions b
        ON e.session_id = b.id
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.duration_sec,
    s.first_known_page,
    s.first_known_page_2,
    s.device,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM joined s
ORDER BY device ASC NULLS LAST;
