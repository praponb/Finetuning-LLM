WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
latest AS (
    -- Keep the latest row per channel, agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, agent_id ORDER BY csat DESC, id DESC) AS rn
        FROM base_tickets
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM latest s
    WHERE s.id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    csat,
    tags,
    CASE WHEN requester_id >= 23 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
    CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group
FROM without_matches
