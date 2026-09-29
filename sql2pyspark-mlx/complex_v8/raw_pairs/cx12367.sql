WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        CASE WHEN requester_id >= 21 THEN 'large' WHEN requester_id >= 8 THEN 'medium' ELSE 'small' END AS requester_id_tier
    FROM tickets
)
SELECT requester_id AS ref_id, agent_id
FROM valid
UNION ALL
SELECT ticket_id AS ref_id, id AS agent_id
FROM ticket_events
WHERE event_type = 'replied'
  AND ticket_id IS NOT NULL
UNION ALL
SELECT requester_id AS ref_id, agent_id
FROM tickets
WHERE ARRAY_CONTAINS(tags, 'refund')
  AND csat BETWEEN 2 AND 4
ORDER BY ref_id DESC
