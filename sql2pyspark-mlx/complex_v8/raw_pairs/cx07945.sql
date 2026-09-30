WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'created') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_type = 'reopened'
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
),
exploded AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.ticket_opened_at,
        s.ticket_csat,
        t.ticket_tag_pos,
        t.ticket_tag
    FROM combined_details s
    LATERAL VIEW POSEXPLODE(s.ticket_tags) t AS ticket_tag_pos, ticket_tag
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_group,
        s.ticket_opened_at,
        s.ticket_csat,
        s.ticket_tag_pos,
        s.ticket_tag,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM exploded s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        actor,
        event_type_group,
        ticket_tag_pos,
        ticket_tag,
        event_id,
        event_event_type,
        event_event_at,
        COALESCE(ticket_csat, 0) AS ticket_csat_filled
    FROM with_children_events
),
latest_per_key AS (
    -- Keep the latest row per ticket_tag
    SELECT
        id,
        actor,
        event_type_group,
        ticket_tag_pos,
        ticket_tag,
        event_id,
        event_event_type,
        event_event_at,
        ticket_csat_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_tag ORDER BY ticket_csat_filled DESC, id DESC, ticket_tag_pos DESC, event_id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
),
screened AS (
    -- Keep rows whose event matches
    SELECT s.*
    FROM latest_per_key s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.id = s.event_id
              AND HOUR(t.event_at) BETWEEN 6 AND 8
        )
)
SELECT
    id,
    actor,
    event_type_group,
    ticket_tag_pos,
    ticket_tag,
    event_id,
    event_event_type,
    event_event_at,
    ticket_csat_filled,
    GREATEST(ticket_tag_pos, ticket_csat_filled) AS greatest_ticket_tag_pos_ticket_csat_filled,
    DATE_FORMAT(event_event_at, 'yyyy-MM') AS event_event_at_label
FROM screened
ORDER BY ticket_tag
