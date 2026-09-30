WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-02-06 12:00:00'
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    priority,
    csat,
    CASE WHEN priority IN ('P4', 'P1') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group
FROM prepared
WHERE priority NOT IN ('P1', 'P3')
ORDER BY priority DESC, csat NULLS LAST
