WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor = 'system'
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM filtered_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM with_counts
),
with_activity_2 AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority = 'P4'
              AND t.requester_id < 15
        )
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    ticket_id,
    COALESCE(actor, 'ALL') AS actor,
    GROUPING(event_type) AS is_total_event_type,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE actor = 'system') AS n_actor_system,
    COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
FROM with_activity_2
GROUP BY ROLLUP(event_type, ticket_id, actor)
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST, actor NULLS LAST
