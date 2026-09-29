WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        CASE WHEN event_type IN ('escalated', 'created') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
),
latest AS (
    -- Keep the latest row per event_type_group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_date,
        event_type_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type_group ORDER BY event_at DESC, id DESC) AS rn
        FROM base_events
    ) ranked
    WHERE rn = 1
),
screened AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority IS NOT NULL
        )
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.agent_id AS ticket_agent_id,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM screened s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_at_date,
    ticket_requester_id,
    ticket_agent_id,
    agent_tier,
    LENGTH(TRIM(agent_tier)) AS agent_tier_len,
    COALESCE(event_type_group, 'UNKNOWN') AS event_type_group_or_unknown
FROM with_parent_ticket
WHERE agent_tier IS NOT NULL
ORDER BY event_at_date, ticket_id NULLS LAST, id
LIMIT 10
