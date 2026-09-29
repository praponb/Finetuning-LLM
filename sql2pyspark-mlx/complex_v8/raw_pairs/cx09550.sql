WITH scoped_tickets AS (
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
        tags,
        requester_id - csat AS requester_id_minus_csat
    FROM tickets
    WHERE csat < 4
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
with_children AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.session_id,
        s.session_end,
        s.session_csat,
        s.duration_min,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM sessions s
    JOIN tickets t ON t.agent_id = s.agent_id AND t.priority = 'P2'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(duration_min) OVER (PARTITION BY agent_id, ticket_priority) AS agent_id_ticket_priority_sum_duration_min,
        MAX(session_csat) OVER (PARTITION BY agent_id, ticket_priority) AS agent_id_ticket_priority_max_session_csat,
        RANK() OVER (ORDER BY session_end ASC) AS session_end_rank,
        session_csat - LAG(session_csat) OVER (ORDER BY session_end ASC, agent_id, session_id, ticket_id) AS session_csat_change,
        MAX(ticket_csat) OVER (PARTITION BY agent_id, ticket_priority) AS agent_id_ticket_priority_max_ticket_csat,
        ROUND(session_csat / NULLIF(SUM(session_csat) OVER (PARTITION BY agent_id, ticket_priority), 0), 4) AS session_csat_share_of_agent_id_ticket_priority
    FROM with_children
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, session_end_rank, ticket_priority, session_csat_change
    FROM ranked
) src
PIVOT (
    AVG(session_csat_change)
    FOR ticket_priority IN ('P4', 'P1')
)
ORDER BY agent_id, session_end_rank
