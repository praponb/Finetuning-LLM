WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
      AND event_type IN ('reopened', 'replied')
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.channel AS ticket_channel
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_channel,
        TO_DATE(event_at) AS event_at_date
    FROM enriched_ticket
    WHERE TO_DATE(event_at) < DATE '2025-03-02'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    ticket_channel,
    event_at_date,
    CASE WHEN ticket_channel IN ('chat', 'phone') THEN 'group_a' WHEN ticket_channel = 'email' THEN 'group_b' ELSE 'other' END AS ticket_channel_group,
    INITCAP(TRIM(ticket_channel)) AS ticket_channel_clean
FROM tagged
ORDER BY id NULLS LAST
