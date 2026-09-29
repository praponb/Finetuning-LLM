WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d
    FROM ticket_events
    WHERE actor = 'system'
      AND ticket_id <= 56
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at_plus_7d) - UNIX_TIMESTAMP(LAG(event_at_plus_7d) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_plus_7d,
        DENSE_RANK() OVER w_actor_by_event_at_ties AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist
    FROM base_events
    WINDOW w_actor_by_event_at_ties AS (PARTITION BY actor ORDER BY event_at)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_plus_7d,
        SUBSTRING(TRIM(event_at_label), 1, 2) AS event_at_label_prefix
    FROM windowed
    WHERE minutes_since_prev_event_at_plus_7d < 3750.0
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM derived s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_id,
    COALESCE(actor, 'ALL') AS actor,
    event_count,
    GROUPING_ID() AS grouping_level,
    MAX(event_at) AS last_event_at
FROM with_counts
GROUP BY ROLLUP(ticket_id, actor, event_count)
