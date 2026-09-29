WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        SUM(csat) AS total_ticket_csat,
        MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.total_ticket_csat,
        t.first_ticket_opened_at
    FROM per_session s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Subtotals with CUBE
SELECT
    n_events,
    agent_id,
    MAX(first_ticket_opened_at) AS last_first_ticket_opened_at,
    COUNT(*) AS n_rows,
    MAX(session_start) AS last_session_start
FROM profiled
GROUP BY CUBE(n_events, agent_id)
ORDER BY n_events NULLS LAST, agent_id NULLS LAST
