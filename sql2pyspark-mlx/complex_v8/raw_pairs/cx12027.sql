WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label
    FROM tickets
    WHERE priority IN ('P1', 'P4')
      AND csat >= 3
)
SELECT csat AS ref_id
FROM base_tickets
WHERE csat <= 4
  AND requester_id <= 20
EXCEPT
SELECT id
FROM ticket_events
WHERE actor = 'agent'
ORDER BY ref_id
