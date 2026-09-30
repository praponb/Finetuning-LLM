WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id >= 9
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        actor,
        ticket_id,
        MAX(days_since_event_at) AS max_days_since_event_at,
        SUM(days_since_event_at) FILTER (WHERE event_type IN ('reopened', 'replied')) AS days_since_event_at_event_type_in_reopened_repli,
        ROUND(SUM(event_at_hour) / NULLIF(SUM(days_since_event_at), 0), 3) AS event_at_hour_to_days_since_event_at_ratio,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        ROUND(AVG(event_at_hour), 2) AS avg_event_at_hour
    FROM filtered
    GROUP BY actor, ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM per_group s
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'assigned'
        )
),
line_level_events AS (
    -- One row per event
    SELECT
        s.actor,
        s.ticket_id,
        s.max_days_since_event_at,
        s.days_since_event_at_event_type_in_reopened_repli,
        s.distinct_event_types,
        s.distinct_ticket_ids,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM qualified s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND HOUR(t.event_at) BETWEEN 7 AND 9
)
-- Final projection
SELECT
    actor,
    ticket_id,
    max_days_since_event_at,
    distinct_event_types,
    distinct_ticket_ids,
    event_id,
    event_actor,
    event_at,
    TO_DATE(event_at) AS event_at_date,
    DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
FROM line_level_events
ORDER BY distinct_ticket_ids DESC, event_at_label
