WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id >= 24
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM valid_events s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        actor,
        event_id,
        event_event_at,
        FIRST_VALUE(event_event_at) IGNORE NULLS OVER (ORDER BY event_event_at DESC NULLS FIRST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_event_at ASC) AS event_event_at_dense_rank
    FROM line_level_events
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, t.event_at
    FROM sequenced s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
joined AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.event_id,
        s.event_event_at,
        s.first_known_event_event_at,
        s.event_event_at_dense_rank,
        s.event_at,
        t.event_type,
        t.ticket_id AS event_ticket_id,
        t.actor AS event_actor,
        ti.tags AS ticket_tags,
        ti.channel AS ticket_channel
    FROM combined_details s
    LEFT JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = t.ticket_id
)
-- Final projection
SELECT DISTINCT ticket_tags
FROM joined
