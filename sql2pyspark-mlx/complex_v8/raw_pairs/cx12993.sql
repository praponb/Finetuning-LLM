WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
      AND HOUR(event_at) BETWEEN 10 AND 14
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel = 'chat'
        )
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.opened_at AS ticket_opened_at, t.agent_id AS ticket_agent_id
    FROM with_activity s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_group,
        s.ticket_agent_id,
        su.tier AS agent_tier
    FROM with_parent_ticket s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_parent s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
)
SELECT
    id,
    ticket_id,
    agent_tier,
    SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
FROM qualified
WHERE event_type_group = 'group_b'
ORDER BY agent_tier
