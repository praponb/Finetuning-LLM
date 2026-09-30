WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.tags AS ticket_tags
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT id, ticket_id, ROW_NUMBER() OVER (ORDER BY event_at, id) AS event_at_seq
    FROM enriched_ticket
)
SELECT
    ticket_id,
    event_at_seq,
    SUM(event_at_seq) AS total_event_at_seq,
    ROUND(STDDEV(event_at_seq), 2) AS stddev_event_at_seq
FROM with_windows
GROUP BY GROUPING SETS ((ticket_id, event_at_seq), (ticket_id))
ORDER BY ticket_id NULLS LAST, event_at_seq NULLS LAST
