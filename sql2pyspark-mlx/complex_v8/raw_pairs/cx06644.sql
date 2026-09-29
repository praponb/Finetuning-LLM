WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE actor = 'system'
      AND event_type IN ('created', 'resolved', 'reopened')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
),
stats AS (
    -- Summarise per group
    SELECT
        event_type,
        SUM(CASE WHEN ticket_priority IS NOT NULL THEN ticket_requester_id ELSE 0 END) AS ticket_requester_id_ticket_priority_is_not_null,
        SUM(CASE WHEN event_type = 'escalated' THEN ticket_requester_id ELSE 0 END) AS ticket_requester_id_event_type_escalated,
        COUNT(*) AS n_rows,
        MAX(event_at) AS last_event_at,
        ROUND(AVG(ticket_requester_id), 2) AS avg_ticket_requester_id
    FROM enriched
    GROUP BY event_type
)
-- Final projection
SELECT
    event_type,
    ticket_requester_id_ticket_priority_is_not_null,
    ticket_requester_id_event_type_escalated,
    n_rows,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
    LOWER(TRIM(event_type)) AS event_type_clean
FROM stats
ORDER BY n_rows
