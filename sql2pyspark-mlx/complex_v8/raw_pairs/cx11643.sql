WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        CASE WHEN event_type IN ('escalated', 'assigned') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type IS NOT NULL
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.actor_event_type_label,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.actor_event_type_label,
        s.ticket_channel,
        s.ticket_agent_id,
        s.ticket_closed_at,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM with_parent s
    JOIN support_agents su ON su.id = s.ticket_agent_id AND su.hired_on IS NOT NULL
    WHERE su.tier <> 'T3'
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.actor_event_type_label,
        s.ticket_agent_id,
        s.ticket_closed_at,
        s.agent_hired_on,
        s.agent_tier,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at_2
    FROM enriched_agent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_event_type_label,
    ticket_agent_id,
    ticket_closed_at,
    agent_hired_on,
    agent_tier,
    ticket_opened_at,
    ticket_closed_at_2 + INTERVAL 30 MINUTES AS ticket_closed_at_2_plus_30m,
    LENGTH(TRIM(actor)) AS actor_len
FROM with_parent_ticket
ORDER BY ticket_closed_at_2_plus_30m DESC
