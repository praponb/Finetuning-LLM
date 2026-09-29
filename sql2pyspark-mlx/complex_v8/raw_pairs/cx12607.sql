WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE ticket_id <= 6
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at_month,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM base s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type = 'escalated'
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at_month,
        s.event_id,
        s.event_event_type,
        s.event_actor,
        t.id AS event_id_2,
        t.event_at,
        t.actor AS event_actor_2,
        t.event_type
    FROM detail_rows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND HOUR(t.event_at) BETWEEN 9 AND 11
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(CASE WHEN actor IN ('agent', 'customer') THEN 1 END) AS event_actor_count,
        COUNT(CASE WHEN actor = 'system' THEN 1 END) AS event_actor_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        COALESCE(t.event_actor_count_2, 0) AS event_actor_count_2
    FROM with_children_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    event_count,
    event_actor,
    event_actor_2,
    COUNT(*) AS row_count,
    COUNT(DISTINCT event_id_2) AS distinct_event_id_2s,
    COUNT(DISTINCT event_id) AS distinct_event_ids
FROM with_stats
GROUP BY GROUPING SETS ((event_count, event_actor, event_actor_2), (event_count), (event_actor), ())
