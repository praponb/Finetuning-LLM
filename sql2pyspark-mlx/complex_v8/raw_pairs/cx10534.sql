WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND event_type = 'escalated'
),
enriched AS (
    -- Label and score rows
    SELECT *, LENGTH(TRIM(event_type)) AS event_type_len
    FROM base
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.event_type_len,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM enriched s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_or_unknown,
        event_id,
        event_actor,
        ROUND(AVG(event_type_len) OVER (PARTITION BY event_actor), 2) AS event_actor_avg_event_type_len,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_event_at DESC, id, event_id) AS event_event_at_dense_rank,
        SIZE(COLLECT_SET(actor_or_unknown) OVER (PARTITION BY event_actor)) AS distinct_actor_or_unknown_in_event_actor
    FROM detail_rows
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.event_id,
        s.event_actor,
        s.event_actor_avg_event_type_len,
        s.distinct_actor_or_unknown_in_event_actor,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        ti.event_type AS event_event_type,
        ti.actor AS event_actor_2
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN ticket_events ti ON ti.id = s.event_id
    WHERE NOT ARRAY_CONTAINS(t.tags, 'bug')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_at, ticket_id, event_event_type, distinct_actor_or_unknown_in_event_actor
    FROM combined_details
) src
PIVOT (
    AVG(distinct_actor_or_unknown_in_event_actor)
    FOR event_event_type IN ('escalated', 'replied', 'reopened', 'created')
)
ORDER BY event_at, ticket_id
