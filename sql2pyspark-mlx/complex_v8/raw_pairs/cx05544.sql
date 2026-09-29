WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label,
        TO_DATE(event_at) AS event_at_date
    FROM base
    WHERE ticket_id IS NOT NULL
),
grouped AS (
    -- Group-level totals
    SELECT
        actor,
        event_type,
        ROUND(AVG(event_type_len), 2) AS avg_event_type_len,
        DATEDIFF(MAX(event_at_date), MIN(event_at_date)) AS event_at_date_span_days
    FROM enriched
    WHERE YEAR(event_at_date) = 2024
    GROUP BY 1, 2
    HAVING AVG(event_type_len) > 6
),
tagged AS (
    -- Derive helper columns
    SELECT
        actor,
        event_type,
        avg_event_type_len,
        event_at_date_span_days,
        COALESCE(event_at_date_span_days, 0) AS event_at_date_span_days_filled,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM grouped
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        actor,
        event_type,
        event_at_date_span_days,
        event_at_date_span_days_filled,
        actor_event_type_label,
        FLOOR(avg_event_type_len / 2) * 2 AS avg_event_type_len_bucket,
        FLOOR(avg_event_type_len / 2) * 2 AS avg_event_type_len_bucket_2
    FROM tagged
)
-- Final projection
SELECT actor, event_type, actor_event_type_label
FROM tagged_2
ORDER BY actor_event_type_label, event_type
