WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
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
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
    WHERE channel IS NOT NULL
)
SELECT priority AS category
FROM filtered_tickets
WHERE opened_at < TIMESTAMP '2025-02-18 13:00:00'
EXCEPT ALL
SELECT channel
FROM tickets
WHERE channel IN ('web', 'chat')
ORDER BY category DESC
