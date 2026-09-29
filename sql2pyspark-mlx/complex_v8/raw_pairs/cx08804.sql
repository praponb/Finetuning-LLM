WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 30
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_no
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_ym,
        event_type,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null,
        MAX(duration_min) AS max_duration_min,
        MIN(duration_min) AS min_duration_min
    FROM sessions
    WHERE duration_min > 60.0
    GROUP BY 1, 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(min_duration_min) OVER (PARTITION BY distinct_event_types ORDER BY distinct_event_types DESC, session_end_ym, event_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_min_duration_min_so_far,
        SUM(max_duration_min) OVER (PARTITION BY distinct_event_types ORDER BY distinct_event_types DESC, session_end_ym, event_type ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS max_duration_min_sum_4,
        SUM(n_event_type_is_not_null) OVER (PARTITION BY distinct_event_types ORDER BY distinct_event_types DESC, session_end_ym, event_type ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS n_event_type_is_not_null_sum_6_prior
    FROM per_group
)
-- Final projection
SELECT
    session_end_ym,
    n_event_type_is_not_null,
    max_min_duration_min_so_far,
    max_duration_min_sum_4,
    n_event_type_is_not_null_sum_6_prior
FROM windowed
ORDER BY n_event_type_is_not_null, max_duration_min_sum_4 NULLS LAST
