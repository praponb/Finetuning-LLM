WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 08:00:00'
      AND ticket_id IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2,
        t.last_event_at
    FROM valid_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', first_event_at) AS days_since_first_event_at,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM with_stats
    WHERE TO_DATE(event_at) < DATE '2025-03-08'
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at_2,
        COUNT(CASE WHEN event_type NOT IN ('reopened', 'assigned') THEN 1 END) AS event_event_type_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at_2,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count
    FROM tagged s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    first_event_at,
    last_event_at,
    days_since_first_event_at,
    event_type_or_unknown,
    actor_group,
    event_event_type_count,
    DAYOFWEEK(event_at) AS event_at_dayofweek
FROM with_counts
WHERE ticket_id IS NOT NULL
ORDER BY event_at DESC, actor NULLS LAST
