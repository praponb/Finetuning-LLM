WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        TO_DATE(event_at) AS event_at_date,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m
    FROM ticket_events
    WHERE actor <> 'system'
      AND event_type NOT IN ('escalated', 'assigned')
),
latest AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_at_date,
        event_at_plus_30m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_date, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at
    FROM latest s
    JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix_2,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_ticket_closed_at
    FROM joined_ticket
),
deduped AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_closed_at,
        actor_prefix,
        actor_prefix_2,
        minutes_event_at_to_ticket_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id DESC) AS rn
        FROM derived
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT DISTINCT event_type, actor_prefix_2, minutes_event_at_to_ticket_closed_at
FROM deduped
ORDER BY actor_prefix_2, event_type
