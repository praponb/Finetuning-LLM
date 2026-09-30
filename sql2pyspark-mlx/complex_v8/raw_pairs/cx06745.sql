WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, session_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(session_end, 'yyyy-MM') AS session_end_label,
        CASE WHEN event_type IN ('assigned', 'escalated') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        ROUND(duration_min * 1.07, 2) AS duration_min_adj
    FROM per_session
    WHERE session_id < 4
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        event_type,
        session_id,
        session_end,
        duration_min,
        session_end_label,
        duration_min_adj,
        MAX(event_count) OVER w_session_start_year_event_typ AS session_start_year_event_type_group_max_event_co,
        MAX(duration_min) OVER (PARTITION BY YEAR(session_start), event_type_group) AS session_start_year_event_type_group_max_duration,
        SUM(duration_min) OVER (PARTITION BY DATE_TRUNC('MONTH', session_end), event_type ORDER BY session_start, event_type, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_min,
        COUNT(*) OVER (PARTITION BY YEAR(session_start), event_type_group) AS rows_in_session_start_year_event_type_group
    FROM derived
    WINDOW w_session_start_year_event_typ AS (PARTITION BY YEAR(session_start), event_type_group)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT session_id, session_end, event_type, rows_in_session_start_year_event_type_group
    FROM windowed
) src
PIVOT (
    COUNT(rows_in_session_start_year_event_type_group)
    FOR event_type IN ('created', 'reopened', 'replied')
)
ORDER BY session_id, session_end
