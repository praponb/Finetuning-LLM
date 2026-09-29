WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE actor NOT IN ('agent', 'customer')) AS event_actor_count,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        COALESCE(t.event_actor_count, 0) AS event_actor_count,
        t.last_event_at
    FROM scoped_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
latest AS (
    -- Keep the earliest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        event_count,
        event_actor_count,
        last_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_count, id) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_id, event_at, actor, event_count
    FROM latest
) src
PIVOT (
    COUNT(event_count)
    FOR actor IN ('agent', 'customer')
)
ORDER BY ticket_id, event_at
