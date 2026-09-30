WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'bug')
),
without_matches AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM prepared_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND HOUR(t.opened_at) BETWEEN 6 AND 11
              AND HOUR(t.closed_at) BETWEEN 6 AND 11
        )
)
SELECT
    priority,
    agent_id,
    channel,
    COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4
FROM without_matches
GROUP BY GROUPING SETS ((priority, agent_id, channel), (priority))
ORDER BY priority NULLS LAST, agent_id NULLS LAST, channel NULLS LAST
