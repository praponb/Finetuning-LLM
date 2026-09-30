WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND event_type = 'created'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.event_type <> 'resolved' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_event_type_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM scoped_events s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on
    FROM with_counts s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_type_or_unknown,
    event_event_type_count,
    last_event_at_2,
    ticket_tags,
    ticket_opened_at,
    agent_hired_on,
    COALESCE(event_event_type_count, 0) AS event_event_type_count_filled,
    ABS(event_event_type_count - 2) AS event_event_type_count_distance
FROM combined_details
WHERE event_event_type_count < 4
ORDER BY event_at DESC NULLS FIRST, actor
