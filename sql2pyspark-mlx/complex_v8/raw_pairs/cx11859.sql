WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('created', 'assigned')
      AND event_at < TIMESTAMP '2025-02-06 12:00:00'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.priority AS ticket_priority
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(ticket_priority, 'ALL') AS ticket_priority,
    COALESCE(actor, 'ALL') AS actor,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM enriched
GROUP BY GROUPING SETS ((ticket_priority, actor), (ticket_priority))
