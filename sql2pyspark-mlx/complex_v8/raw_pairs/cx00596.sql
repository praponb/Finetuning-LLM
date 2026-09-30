WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM ticket_events
    WHERE event_type NOT IN ('escalated', 'reopened')
      AND MONTH(event_at) IN (1, 2)
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_hour,
        t.opened_at AS ticket_opened_at
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
),
stats AS (
    -- Summarise per group
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE actor = 'system') AS n_actor_system,
        COUNT(*) FILTER (WHERE event_type IN ('replied', 'escalated', 'resolved')) AS n_event_type_in_replied_escala,
        COUNT(*) AS row_count,
        COUNT(DISTINCT actor) AS distinct_actors,
        SUM(CASE WHEN event_type = 'reopened' THEN 1 ELSE 0 END) AS n_event_type_reopened
    FROM combined_details_ticket
    GROUP BY ticket_id
)
SELECT ticket_id, field, amount
FROM (
    SELECT ticket_id, row_count, n_event_type_in_replied_escala
    FROM stats
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (row_count, n_event_type_in_replied_escala)
) unp
ORDER BY ticket_id, field
