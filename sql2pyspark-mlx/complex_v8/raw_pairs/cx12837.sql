WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug_2
    FROM ticket_events
    WHERE ticket_id <= 36
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_slug_2,
        t.agent_id AS ticket_agent_id,
        t.opened_at AS ticket_opened_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id <= 8
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_agent_id,
        s.ticket_opened_at,
        t.channel AS ticket_channel
    FROM joined s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    ticket_id,
    ticket_agent_id,
    GROUPING(ticket_id) AS is_total_ticket_id,
    COUNT(*) AS n_rows
FROM enriched
GROUP BY GROUPING SETS ((ticket_id, ticket_agent_id), (ticket_id), (ticket_agent_id), ())
ORDER BY ticket_id NULLS LAST, ticket_agent_id NULLS LAST
