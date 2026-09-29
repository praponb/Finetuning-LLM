WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM recent_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        priority,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_no
),
labeled AS (
    -- Add calculated columns
    SELECT
        priority,
        session_no,
        events,
        duration_min,
        MONTH(session_end) AS session_end_month,
        duration_min + session_no AS duration_min_plus_session_no,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket
    FROM sessions
)
SELECT
    priority,
    session_no,
    events,
    duration_min,
    session_end_month,
    duration_min_plus_session_no,
    CASE WHEN events >= 4 THEN 'large' WHEN events >= 2 THEN 'medium' ELSE 'small' END AS events_tier,
    LENGTH(TRIM(priority)) AS priority_len
FROM labeled
ORDER BY duration_min NULLS LAST, session_no, priority
LIMIT 10
