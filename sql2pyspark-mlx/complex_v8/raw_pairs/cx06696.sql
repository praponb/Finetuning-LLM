WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor = 'customer'
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(CASE WHEN t.actor <> 'customer' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM prepared_events s
),
totals AS (
    -- Summarise per group
    SELECT
        actor,
        DATE_TRUNC('MONTH', event_at_date) AS event_at_date_month,
        ROUND(100 * COUNT(CASE WHEN actor = 'system' THEN 1 END) / COUNT(*), 1) AS pct_actor_system,
        SUM(event_actor_count) FILTER (WHERE event_type = 'replied') AS event_actor_count_event_type_replied,
        SUM(CASE WHEN event_type NOT IN ('resolved', 'created') THEN event_actor_count ELSE 0 END) AS event_actor_count_event_type_not_in_resolved_c,
        ROUND(100 * COUNT(CASE WHEN event_type = 'assigned' THEN 1 END) / COUNT(*), 1) AS pct_event_type_assigned,
        PERCENTILE_APPROX(event_actor_count, 0.5) AS median_event_actor_count,
        COUNT(*) AS row_count
    FROM profiled
    GROUP BY 1, 2
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(actor)) AS actor_clean,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        UPPER(TRIM(actor)) AS actor_clean_2
    FROM totals
    WHERE pct_actor_system <= 75.0
)
-- Final projection
SELECT
    actor,
    pct_actor_system,
    event_actor_count_event_type_replied,
    pct_event_type_assigned,
    row_count,
    actor_clean,
    actor_prefix,
    LOWER(TRIM(actor_clean_2)) AS actor_clean_2_clean,
    TO_DATE(event_at_date_month) AS event_at_date_month_date
FROM scored
WHERE pct_event_type_assigned >= 20.0
ORDER BY pct_event_type_assigned, actor_clean NULLS LAST
