WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-10 21:00:00'
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM cleaned_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND TO_DATE(t.opened_at) < DATE '2025-02-06'
        )
)
SELECT
    agent_id,
    channel,
    priority,
    SUM(csat) AS total_csat
FROM screened
GROUP BY GROUPING SETS ((agent_id, channel, priority), (agent_id))
ORDER BY agent_id NULLS LAST, channel NULLS LAST, priority NULLS LAST
