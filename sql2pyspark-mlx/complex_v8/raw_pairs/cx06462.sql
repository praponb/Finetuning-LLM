WITH recent AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per event_type, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id
    FROM deduped s
    JOIN tickets t ON t.id = s.ticket_id AND NOT ARRAY_CONTAINS(t.tags, 'upgrade')
    WHERE t.requester_id > 2
)
SELECT
    ticket_id,
    actor,
    event_type,
    COUNT(*) FILTER (WHERE event_type = 'created') AS n_event_type_created
FROM combined_details
GROUP BY GROUPING SETS ((ticket_id, actor, event_type), (ticket_id), ())
ORDER BY ticket_id NULLS LAST, actor NULLS LAST, event_type NULLS LAST
