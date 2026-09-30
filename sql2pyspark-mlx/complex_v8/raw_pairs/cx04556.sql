WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
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
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
),
with_counts AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id,
        (SELECT COUNT(*) FILTER (WHERE t.priority = 'P3') FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at
    FROM per_session s
)
SELECT agent_id, session_id, metric, reading
FROM (
    SELECT agent_id, session_id, min_ticket_requester_id, ticket_priority_count, events
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric IN (min_ticket_requester_id, ticket_priority_count, events)
) unp
