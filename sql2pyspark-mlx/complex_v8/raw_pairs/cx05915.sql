WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        HOUR(event_at) AS event_at_hour,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_type <> 'reopened'
      AND ticket_id >= 43
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_at_hour,
        event_type_ref,
        ROUND(event_at_hour - AVG(event_at_hour) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type), 2) AS event_at_hour_vs_event_at_month_event_type_avg,
        ROW_NUMBER() OVER (PARTITION BY event_at_hour ORDER BY event_at DESC NULLS FIRST, id) AS event_at_seq,
        LAG(event_at_hour) OVER (PARTITION BY event_at_hour ORDER BY event_at DESC NULLS FIRST, id) AS prev_event_at_hour
    FROM valid_events
),
labeled AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('HOUR', event_at) AS event_at_hour_2
    FROM ranked
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_at_hour,
    event_type_ref,
    event_at_hour_vs_event_at_month_event_type_avg,
    event_at_seq,
    prev_event_at_hour,
    event_at_hour_2,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM labeled
ORDER BY id NULLS LAST, event_type_or_unknown
