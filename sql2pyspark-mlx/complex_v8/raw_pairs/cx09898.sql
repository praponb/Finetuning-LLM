WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per priority
    SELECT *, LAG(closed_at) OVER (PARTITION BY priority ORDER BY closed_at, id) AS prev_closed_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        burst_id,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, burst_id
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM per_session
    WHERE burst_id > (SELECT MIN(burst_id) FROM per_session)
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE session_requester_id > (SELECT MIN(session_requester_id) FROM filtered)
)
SELECT
    priority,
    session_start,
    session_requester_id,
    duration_min,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start,
    ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start_2
FROM without_matches
WHERE priority IN ('P3', 'P4')
ORDER BY priority, minutes_session_end_to_session_start
