WITH cleaned AS (
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
    WHERE csat > 2
      AND priority IN ('P2', 'P4', 'P3')
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.csat < 4
        )
)
-- Subtotals with CUBE
SELECT
    priority,
    agent_id,
    MAX(csat) AS max_csat
FROM with_activity
GROUP BY CUBE(priority, agent_id)
ORDER BY priority NULLS LAST, agent_id NULLS LAST
