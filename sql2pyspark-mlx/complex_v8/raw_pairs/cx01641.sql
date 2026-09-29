WITH prepared_tickets AS (
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
        tags,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE requester_id >= 7
)
SELECT csat
FROM prepared_tickets
WHERE closed_at < TIMESTAMP '2025-02-20 15:00:00'
EXCEPT ALL
SELECT id
FROM support_agents
WHERE hired_on < DATE '2025-02-26'
ORDER BY csat
