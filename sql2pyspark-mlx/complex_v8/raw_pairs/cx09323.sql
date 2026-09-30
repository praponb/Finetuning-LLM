WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type = 'resolved'
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM filtered_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(event_type_len) AS session_event_type_len,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        event_type,
        burst_id,
        session_start,
        session_end,
        session_event_type_len,
        duration_min,
        ROW_NUMBER() OVER (PARTITION BY event_count, burst_id ORDER BY session_start ASC, event_type, burst_id) AS seq_no,
        FIRST_VALUE(burst_id) IGNORE NULLS OVER (PARTITION BY event_count, burst_id ORDER BY session_start ASC, event_type, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_burst_id,
        ROUND(PERCENT_RANK() OVER w_event_count_burst_id_by_sess, 3) AS session_start_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_count, burst_id ORDER BY session_start ASC), 3) AS session_start_percent_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY event_count, burst_id ORDER BY session_start ASC), 3) AS session_start_cume_dist
    FROM sessions
    WINDOW w_event_count_burst_id_by_sess AS (PARTITION BY event_count, burst_id ORDER BY session_start ASC, event_type, burst_id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_start, first_known_burst_id, event_type, session_start_cume_dist
    FROM with_trends
) src
PIVOT (
    COUNT(session_start_cume_dist)
    FOR event_type IN ('escalated', 'created', 'assigned', 'replied')
)
