WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND ticket_id <= 22
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id) AS event_at_dense_rank,
        NTILE(5) OVER w_actor_event_type_by_event_at AS event_at_quintile,
        RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at ASC) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id), 3) AS event_at_cume_dist
    FROM prepared_events
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at ASC, id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        COALESCE(t.event_count, 0) AS event_count
    FROM ranked s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    event_count,
    GROUPING(event_type) AS is_total_event_type,
    ROUND(AVG(event_at_quintile), 2) AS avg_event_at_quintile
FROM profiled
GROUP BY ROLLUP(event_type, event_count)
ORDER BY event_type NULLS LAST, event_count NULLS LAST
