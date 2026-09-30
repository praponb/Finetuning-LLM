WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        tags,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref
    FROM tickets
    WHERE agent_id <= 9
      AND csat <= 4
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        su.hired_on AS agent_hired_on
    FROM base s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE YEAR(su.hired_on) = 2025
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_opened_at
    FROM with_details s
)
SELECT
    ticket_count,
    agent_id,
    COALESCE(channel, 'ALL') AS channel,
    MIN(requester_id) AS min_requester_id,
    COUNT(*) AS row_count,
    MAX(min_ticket_requester_id) AS max_min_ticket_requester_id
FROM profiled
GROUP BY CUBE(ticket_count, agent_id, channel)
ORDER BY ticket_count NULLS LAST, agent_id NULLS LAST, channel NULLS LAST
