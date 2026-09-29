WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'created') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
    WHERE (actor IS NOT NULL OR MONTH(event_at) IN (1, 3))
),
latest AS (
    -- Keep the earliest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_group,
        actor_ref,
        event_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS recency_rank
        FROM cleaned_events
    ) ranked
    WHERE recency_rank = 1
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_type_group,
        s.actor_ref,
        s.event_type_prefix,
        t.tags AS ticket_tags
    FROM latest s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND SIZE(t.tags) >= 1
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type_group,
        s.actor_ref,
        s.ticket_tags,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at
    FROM with_details s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_type_group,
    ticket_tags,
    event_id,
    event_event_type,
    event_at,
    YEAR(event_at) AS event_at_year
FROM exploded_detail
WHERE ticket_id IS NOT NULL
ORDER BY id DESC, event_event_type NULLS LAST
