WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM ticket_events
    WHERE event_type IN ('resolved', 'created', 'reopened')
      AND ticket_id IS NOT NULL
),
enriched AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_actor_label,
        s.event_at_day,
        t.closed_at AS ticket_closed_at,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    event_at_day,
    agent_tier,
    agent_hired_on,
    CONCAT_WS('|', event_type_actor_label, event_type) AS event_type_actor_label_event_type_label,
    ticket_closed_at + INTERVAL 30 MINUTES AS ticket_closed_at_plus_30m
FROM enriched
ORDER BY event_type_actor_label_event_type_label, agent_tier
