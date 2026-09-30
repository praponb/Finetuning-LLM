WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        UPPER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE (HOUR(event_at) BETWEEN 6 AND 8 OR ticket_id >= 4)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        days_since_event_at,
        event_type_clean,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_event_type, 0), 4) AS days_since_event_at_share_of_event_type,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY days_since_event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        SIZE(COLLECT_SET(actor) OVER w_event_type) AS distinct_actor_in_event_type
    FROM filtered_events
    WINDOW w_event_type AS (PARTITION BY event_type)
)
SELECT id, metric_name, value
FROM (
    SELECT id, minutes_since_prev_event_at, days_since_event_at_share_of_event_type
    FROM with_trends
) src
UNPIVOT (
    value FOR metric_name IN (minutes_since_prev_event_at, days_since_event_at_share_of_event_type)
) unp
