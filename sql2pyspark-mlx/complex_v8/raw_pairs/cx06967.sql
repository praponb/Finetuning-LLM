WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor IN ('customer', 'agent') OR event_type NOT IN ('assigned', 'created'))
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.csat AS ticket_csat
    FROM scoped_events s
    JOIN tickets t ON t.id = s.ticket_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM joined s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_csat,
    ticket_id,
    event_type,
    SUM(ticket_csat) AS total_ticket_csat,
    MIN(ticket_csat) AS min_ticket_csat,
    COUNT(DISTINCT event_type) AS distinct_event_types
FROM screened
GROUP BY GROUPING SETS ((ticket_csat, ticket_id, event_type), (ticket_csat), ())
ORDER BY ticket_csat NULLS LAST, ticket_id NULLS LAST, event_type NULLS LAST
