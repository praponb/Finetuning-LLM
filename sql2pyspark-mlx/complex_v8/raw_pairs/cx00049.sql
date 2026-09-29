WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        tags
    FROM tickets
    WHERE MONTH(closed_at) IN (2, 3)
      AND channel NOT IN ('chat', 'phone')
),
combined_details_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name, su.team AS agent_team
    FROM filtered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details_agent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at < TIMESTAMP '2025-03-08 09:00:00'
              AND t.actor IN ('system', 'agent')
        )
)
SELECT *
FROM (
    SELECT opened_at, agent_team, priority, requester_id
    FROM with_activity
) src
PIVOT (
    SUM(requester_id) AS total, MAX(requester_id) AS max_v
    FOR priority IN ('P1', 'P3')
)
ORDER BY opened_at, agent_team
