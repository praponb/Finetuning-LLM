WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor = 'customer')
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM scoped_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor NOT IN ('agent', 'system')
        )
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM with_activity
    WHERE event_type NOT IN ('assigned', 'created')
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.days_since_event_at,
        s.event_type_prefix,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM annotated s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', event_event_at) AS event_event_at_month,
        event_actor,
        event_id,
        SUM(days_since_event_at) FILTER (WHERE event_actor IN ('system', 'agent')) AS days_since_event_at_event_actor_in_system_agent,
        MAX(days_since_event_at) AS max_days_since_event_at
    FROM line_level_events
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        event_event_at_month,
        event_actor,
        event_id,
        max_days_since_event_at,
        MIN(max_days_since_event_at) OVER (PARTITION BY event_actor, event_id) AS event_actor_event_id_min_max_days_since_event_at,
        SUM(max_days_since_event_at) OVER (PARTITION BY event_actor, event_id) AS event_actor_event_id_sum_max_days_since_event_at
    FROM stats
)
-- Final projection
SELECT
    event_event_at_month,
    event_actor,
    event_actor_event_id_sum_max_days_since_event_at,
    DATE_FORMAT(event_event_at_month, 'yyyy-MM') AS event_event_at_month_label,
    FLOOR(max_days_since_event_at / 100) * 100 AS max_days_since_event_at_bucket
FROM ranked
ORDER BY event_actor, event_actor_event_id_sum_max_days_since_event_at DESC
