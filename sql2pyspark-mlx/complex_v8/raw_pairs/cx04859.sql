WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id <= 10
      AND HOUR(event_at) BETWEEN 10 AND 16
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.tags AS ticket_tags
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at
    FROM joined s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.opened_at >= TIMESTAMP '2025-01-25 11:00:00'
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_parent_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
              AND t.channel IN ('chat', 'phone', 'email')
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        CASE WHEN ticket_requester_id >= 15 THEN 'HIGH' WHEN ticket_requester_id >= 8 THEN 'MEDIUM' ELSE 'LOW' END AS ticket_requester_id_tier
    FROM qualified
    WHERE event_type = 'created'
),
grouped AS (
    -- Group-level totals
    SELECT
        actor,
        ticket_agent_id,
        COUNT(*) AS cnt,
        MAX(ticket_requester_id) AS max_ticket_requester_id,
        COUNT(DISTINCT ticket_requester_id_tier) AS distinct_ticket_requester_id_tiers
    FROM derived
    GROUP BY actor, ticket_agent_id
)
-- Final projection
SELECT
    actor,
    cnt,
    max_ticket_requester_id,
    distinct_ticket_requester_id_tiers
FROM grouped
WHERE max_ticket_requester_id >= 11
ORDER BY distinct_ticket_requester_id_tiers, actor DESC NULLS FIRST
