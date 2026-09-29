WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat
    FROM tickets
    WHERE channel = 'chat'
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
joined AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name
    FROM per_session s
    JOIN support_agents su ON su.id = s.agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.channel = 'chat' THEN 1 END) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM joined s
)
-- Final projection
SELECT
    agent_id,
    session_start,
    session_requester_id,
    duration_min,
    agent_name,
    ticket_channel_count,
    first_ticket_closed_at,
    ticket_count
FROM with_stats
WHERE agent_id <= 6
ORDER BY duration_min, ticket_channel_count DESC
