WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE actor IS NOT NULL
),
without_matches AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM valid_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND HOUR(t.closed_at) BETWEEN 8 AND 14
              AND t.priority IS NOT NULL
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count,
        MAX(event_at) AS last_event_at,
        COUNT(*) AS event_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count,
        t.last_event_at,
        COALESCE(t.event_count_2, 0) AS event_count_2
    FROM without_matches s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_count DESC), 3) AS event_count_cume_dist,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_count DESC, id) AS event_count_rank
    FROM with_stats
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, event_count, event_count_rank
    FROM with_trends
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (event_count, event_count_rank)
) unp
