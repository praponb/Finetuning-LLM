WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_slug,
        actor_ref,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY event_type, ticket_id), 2) AS days_since_event_at_vs_event_type_ticket_id_avg,
        LAST_VALUE(days_since_event_at) IGNORE NULLS OVER (ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_event_at
    FROM recent
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM windowed s
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_slug,
        s.days_since_event_at_vs_event_type_ticket_id_avg,
        s.last_known_days_since_event_at,
        s.last_event_at,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at
    FROM summarized_rows s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_type_slug,
    last_known_days_since_event_at,
    last_event_at,
    ticket_channel,
    ticket_agent_id,
    ticket_opened_at
FROM combined_details_ticket
WHERE event_type_slug IS NOT NULL
ORDER BY ticket_channel, last_known_days_since_event_at DESC, id
LIMIT 10
