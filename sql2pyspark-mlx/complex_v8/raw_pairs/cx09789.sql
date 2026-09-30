WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 10:00:00'
      AND event_type IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(event_at_plus_15m) OVER (PARTITION BY actor_group ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_plus_15m,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at, id), 3) AS event_at_percent_rank
    FROM cleaned_events
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-25 08:00:00'
              AND t.event_type IN ('reopened', 'escalated')
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', event_at_plus_15m) AS days_since_event_at_plus_15m,
        CONCAT_WS('-', event_type, event_at_label) AS event_type_event_at_label_label
    FROM qualified
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_group,
        s.event_at_label,
        s.first_event_at_plus_15m,
        s.days_since_event_at_plus_15m,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM scored s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'system'
),
with_activity AS (
    -- Keep rows whose event matches
    SELECT s.*
    FROM with_children_events s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.id = s.event_id
              AND HOUR(t.event_at) BETWEEN 8 AND 10
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor_group,
    event_at_label,
    first_event_at_plus_15m,
    days_since_event_at_plus_15m,
    event_id,
    event_event_type,
    event_actor
FROM with_activity
WHERE event_id >= 32
ORDER BY id NULLS LAST, days_since_event_at_plus_15m
