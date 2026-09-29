WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 13:00:00'
        )
),
latest_per_key AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM screened
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d
    FROM latest_per_key
    WHERE ticket_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_actor_label,
        s.event_type_ref,
        s.event_at_plus_1d,
        t.closed_at AS ticket_closed_at,
        t.requester_id AS ticket_requester_id
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_type_actor_label,
    event_at_plus_1d,
    ticket_closed_at,
    ticket_requester_id,
    CONCAT_WS(' / ', event_type_actor_label, event_type) AS event_type_actor_label_event_type_label
FROM combined_details
ORDER BY ticket_closed_at DESC NULLS FIRST
