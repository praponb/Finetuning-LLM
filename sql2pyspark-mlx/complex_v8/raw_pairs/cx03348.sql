WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 10 AND 12
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM cleaned s
)
SELECT
    agent_id,
    ticket_count,
    COALESCE(priority, 'ALL') AS priority,
    GROUPING(agent_id) AS is_total_agent_id,
    SUM(ticket_count) AS total_ticket_count
FROM with_stats
GROUP BY GROUPING SETS ((agent_id, ticket_count, priority), (agent_id), ())
