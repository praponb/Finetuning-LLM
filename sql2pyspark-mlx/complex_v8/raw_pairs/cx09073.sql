WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        DATE_TRUNC('HOUR', closed_at) AS closed_at_hour
    FROM tickets
),
with_prev AS (
    -- Previous closed_at_hour per agent_id
    SELECT *, LAG(closed_at_hour) OVER (PARTITION BY agent_id ORDER BY closed_at_hour, id) AS prev_closed_at_hour
    FROM cleaned_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at_hour IS NULL OR closed_at_hour > prev_closed_at_hour + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY closed_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(closed_at_hour) AS session_start,
        MAX(closed_at_hour) AS session_end,
        COUNT(*) AS n_events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at_hour)) - UNIX_TIMESTAMP(MIN(closed_at_hour))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
)
SELECT
    agent_id,
    session_no,
    session_start,
    session_end,
    n_events,
    duration_min,
    CASE WHEN session_requester_id >= 60 THEN 'HIGH' WHEN session_requester_id >= 31 THEN 'MEDIUM' ELSE 'LOW' END AS session_requester_id_tier,
    duration_min - session_requester_id AS duration_min_minus_session_requester_id
FROM session_summary
ORDER BY agent_id
