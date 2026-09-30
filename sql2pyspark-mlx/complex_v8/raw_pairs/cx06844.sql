WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('escalated', 'replied') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        MONTH(event_at) AS event_at_month,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE actor <> 'agent'
      AND HOUR(event_at) BETWEEN 10 AND 12
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_group,
        s.event_at_month,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM base s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'system'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type_group,
    event_at_month,
    event_id,
    event_actor,
    event_event_type,
    COALESCE(event_at_month, 0) AS event_at_month_filled,
    LENGTH(TRIM(event_event_type)) AS event_event_type_len
FROM with_children_events
ORDER BY event_event_type DESC NULLS FIRST
