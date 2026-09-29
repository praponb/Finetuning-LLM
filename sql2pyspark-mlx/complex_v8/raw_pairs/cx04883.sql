WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IS NOT NULL
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_no
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(event_type)) AS event_type_len,
        GREATEST(duration_min, session_no) AS greatest_duration_min_session_no,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM sessions
),
annotated AS (
    -- Add calculated columns
    SELECT
        event_type,
        session_no,
        session_start,
        session_end,
        event_count,
        event_type_len,
        greatest_duration_min_session_no,
        event_type_slug,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown_2,
        DAYOFWEEK(session_end) AS session_end_dayofweek
    FROM enriched
)
SELECT
    event_type_or_unknown,
    event_type,
    session_end_dayofweek,
    GROUPING_ID() AS gid,
    MIN(session_end_dayofweek) AS min_session_end_dayofweek,
    COUNT(*) AS n_rows
FROM annotated
GROUP BY GROUPING SETS ((event_type_or_unknown, event_type, session_end_dayofweek), (event_type_or_unknown), (event_type))
ORDER BY event_type_or_unknown NULLS LAST, event_type NULLS LAST, session_end_dayofweek NULLS LAST
