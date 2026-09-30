WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND ticket_id <= 48
),
enriched AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(event_type)) AS event_type_len
    FROM scoped
    WHERE actor = 'agent'
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_len,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    event_type_len,
    ticket_id,
    ticket_priority,
    MIN(event_type_len) AS min_event_type_len,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM joined
GROUP BY GROUPING SETS ((event_type_len, ticket_id, ticket_priority), (event_type_len), (ticket_id))
ORDER BY event_type_len NULLS LAST, ticket_id NULLS LAST, ticket_priority NULLS LAST
