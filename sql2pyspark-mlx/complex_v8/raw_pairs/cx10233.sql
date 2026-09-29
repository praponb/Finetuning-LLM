WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 09:00:00'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at
    FROM scoped s
),
latest_per_key AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_prefix,
        event_type_ref,
        last_event_at,
        first_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY last_event_at, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_prefix,
        s.event_type_ref,
        s.last_event_at,
        s.first_event_at,
        t.tags AS ticket_tags
    FROM latest_per_key s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE NOT ARRAY_CONTAINS(t.tags, 'upgrade')
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_prefix,
    event_type_ref,
    last_event_at,
    first_event_at,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM joined
WHERE event_at < TIMESTAMP '2025-02-24 10:00:00'
ORDER BY first_event_at DESC, event_type_ref DESC
