WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id <= 7
      AND event_type IS NOT NULL
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MIN(event_at) AS first_event_at_2,
        COUNT(CASE WHEN actor = 'system' THEN 1 END) AS event_actor_count
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.first_event_at_2,
        COALESCE(t.event_actor_count, 0) AS event_actor_count
    FROM base_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_event_at_2) AS first_event_at_2_month,
        event_type,
        YEAR(event_at) AS event_at_year,
        COUNT(*) AS cnt,
        COUNT(DISTINCT actor) AS distinct_actors,
        MIN(first_event_at_2) AS first_first_event_at_2,
        ROUND(SUM(event_actor_count) / NULLIF(SUM(actor_len), 0), 3) AS event_actor_count_to_actor_len_ratio
    FROM with_stats
    GROUP BY 1, 2, 3
)
SELECT
    first_event_at_2_month,
    event_type,
    cnt,
    event_actor_count_to_actor_len_ratio,
    SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
FROM grouped
ORDER BY event_actor_count_to_actor_len_ratio DESC NULLS FIRST, cnt NULLS LAST
