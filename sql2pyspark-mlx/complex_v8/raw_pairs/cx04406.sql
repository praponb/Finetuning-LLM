WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE (event_type NOT IN ('resolved', 'escalated') OR HOUR(event_at) BETWEEN 8 AND 14)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
joined AS (
    -- Join ticket_events
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_id,
        s.event_actor,
        s.event_event_at,
        t.event_type AS event_event_type,
        t.ticket_id AS event_ticket_id,
        ti.tags AS ticket_tags
    FROM detail_rows_events s
    LEFT JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = s.ticket_id
),
combined_details AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_id,
        s.event_actor,
        s.event_event_at,
        s.event_ticket_id,
        s.ticket_tags,
        t.ticket_id AS event_ticket_id_2
    FROM joined s
    JOIN ticket_events t ON t.id = s.event_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.event_ticket_id
        )
      AND s.event_actor = 'agent'
)
-- Final projection
SELECT
    id,
    event_id,
    event_actor,
    event_event_at,
    event_ticket_id,
    ticket_tags,
    event_ticket_id_2,
    CASE WHEN event_actor IN ('agent', 'system') THEN 'group_a' WHEN event_actor = 'customer' THEN 'group_b' ELSE 'other' END AS event_actor_group
FROM screened
ORDER BY event_actor_group DESC NULLS FIRST
