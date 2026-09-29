WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor NOT IN ('system', 'customer')
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
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
metrics AS (
    -- Window metrics over several partitions
    SELECT
        event_type,
        session_no,
        session_start,
        session_end,
        LAST_VALUE(session_no) IGNORE NULLS OVER (PARTITION BY session_no ORDER BY session_end, event_type, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_session_no,
        ROUND(AVG(event_count) OVER w_event_type_event_count, 2) AS event_type_event_count_avg_event_count
    FROM session_summary
    WINDOW w_event_type_event_count AS (PARTITION BY event_type, event_count)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        event_type,
        session_no,
        session_start,
        last_known_session_no,
        event_type_event_count_avg_event_count,
        ROUND(CUME_DIST() OVER (PARTITION BY session_no ORDER BY session_start DESC, event_type, session_no), 3) AS session_start_cume_dist,
        LEAD(last_known_session_no) OVER (PARTITION BY session_no ORDER BY session_start DESC, event_type, session_no) AS next_last_known_session_no,
        LEAD(session_start, 2) OVER (PARTITION BY session_no ORDER BY session_start DESC, event_type, session_no) AS next2_session_start,
        LAG(session_no, 1, 0) OVER (PARTITION BY session_no ORDER BY session_start DESC, event_type, session_no) AS prev_session_no,
        COUNT(*) OVER (PARTITION BY session_no, event_type) AS rows_in_session_no_event_type,
        ROUND(AVG(session_no) OVER (PARTITION BY session_no ORDER BY session_start DESC, event_type, session_no ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS session_no_avg_3_prior
    FROM metrics
)
SELECT
    event_type,
    session_start,
    last_known_session_no,
    event_type_event_count_avg_event_count,
    session_start_cume_dist,
    next_last_known_session_no,
    prev_session_no,
    rows_in_session_no_event_type,
    session_no_avg_3_prior
FROM sequenced
WHERE event_type = 'reopened'
ORDER BY session_no_avg_3_prior DESC, session_start_cume_dist
