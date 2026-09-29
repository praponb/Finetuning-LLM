WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('replied', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM prepared_events
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM ranked s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
deduped AS (
    -- Keep the latest row per event_type, event_type_group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_group,
        second_event_at,
        second_event_at_2,
        first_known_event_at,
        event_at_rank,
        first_event_at,
        event_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, event_type_group ORDER BY first_known_event_at DESC, id DESC) AS rn
        FROM with_stats
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    second_event_at,
    second_event_at_2,
    first_known_event_at,
    event_at_rank,
    first_event_at,
    TO_DATE(first_event_at) AS first_event_at_date
FROM deduped
