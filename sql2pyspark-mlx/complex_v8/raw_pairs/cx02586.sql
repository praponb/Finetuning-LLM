WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        HOUR(event_at) AS event_at_hour,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at_2
    FROM ticket_events
),
ticket_id_stats AS (
    -- Median days_since_event_at per ticket_id
    SELECT ticket_id, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM scoped
    GROUP BY ticket_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM scoped s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.days_since_event_at >= g.median_days_since_event_at
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour_2,
        COALESCE(event_at_hour, 0) AS event_at_hour_filled,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM vs_group
)
SELECT
    actor,
    event_at_hour,
    ticket_id,
    COUNT(*) FILTER (WHERE event_type = 'reopened') AS n_event_type_reopened,
    ROUND(100 * COUNT(CASE WHEN actor IN ('customer', 'system') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_customer_system,
    ROUND(SUM(days_since_event_at) / NULLIF(SUM(days_since_event_at_2), 0), 3) AS days_since_event_at_to_days_since_event_at_2_rat
FROM scored
GROUP BY ROLLUP(actor, event_at_hour, ticket_id)
ORDER BY actor NULLS LAST, event_at_hour NULLS LAST, ticket_id NULLS LAST
