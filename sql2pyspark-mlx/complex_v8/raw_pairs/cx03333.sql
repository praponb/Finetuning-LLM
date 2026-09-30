WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR (UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(prev_closed_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_no,
        MIN(closed_at) AS session_start,
        MAX(closed_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(closed_at)) - UNIX_TIMESTAMP(MIN(closed_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_no
),
annotated AS (
    -- Label and score rows
    SELECT
        agent_id,
        session_no,
        session_start,
        session_end,
        session_csat,
        duration_min,
        session_end + INTERVAL 15 MINUTES AS session_end_plus_15m
    FROM sessions
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.priority NOT IN ('P4', 'P1')) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM annotated s
)
SELECT
    agent_id,
    session_no,
    session_start,
    session_csat,
    duration_min,
    session_end_plus_15m,
    ticket_priority_count,
    last_ticket_closed_at,
    DAYOFWEEK(session_start) AS session_start_dayofweek,
    DATE_TRUNC('YEAR', last_ticket_closed_at) AS last_ticket_closed_at_year
FROM profiled
ORDER BY last_ticket_closed_at
