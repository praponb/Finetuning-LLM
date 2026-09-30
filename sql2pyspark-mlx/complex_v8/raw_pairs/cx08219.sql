WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
scored AS (
    -- Add calculated columns
    SELECT
        event_type,
        burst_id,
        session_start,
        duration_min,
        session_start + INTERVAL 2 HOURS AS session_start_plus_2h,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM session_summary
    WHERE burst_id > 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        QUARTER(session_start_plus_2h) AS session_start_plus_2h_quarter,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        session_start + INTERVAL 1 DAYS AS session_start_plus_1d
    FROM scored
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_start_plus_2h_quarter, event_type_or_unknown, event_type_len
    FROM labeled
) src
PIVOT (
    SUM(event_type_len)
    FOR event_type_or_unknown IN ('replied', 'UNKNOWN', 'resolved', 'escalated')
)
