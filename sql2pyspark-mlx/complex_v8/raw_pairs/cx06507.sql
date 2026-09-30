WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label
    FROM ticket_events
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_label,
        s.actor_event_type_label,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(ticket_channel)) AS ticket_channel_clean,
        CONCAT(event_type, '#', CAST(ticket_agent_id AS STRING)) AS event_type_ref
    FROM joined_ticket
    WHERE ticket_id IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE ticket_channel = 'phone') AS n_ticket_channel_phone,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) FILTER (WHERE ticket_channel IN ('chat', 'email', 'phone')) AS n_ticket_channel_in_chat_email,
        ARRAY_SORT(COLLECT_SET(ticket_channel)) AS ticket_channel_list
    FROM derived
    GROUP BY ticket_id
)
-- Final projection
SELECT
    ticket_id,
    n_ticket_channel_phone,
    distinct_event_types,
    FLOOR(n_ticket_channel_phone / 2) * 2 AS n_ticket_channel_phone_bucket
FROM totals
WHERE SIZE(ticket_channel_list) >= 1
ORDER BY n_ticket_channel_phone_bucket
