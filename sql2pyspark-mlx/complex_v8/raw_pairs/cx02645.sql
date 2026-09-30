WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type = 'replied'
      AND actor = 'customer'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        actor,
        event_at_week,
        days_since_event_at,
        NTILE(10) OVER (ORDER BY event_at_week ASC, id) AS event_at_week_decile,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_week ASC, id), 3) AS event_at_week_cume_dist,
        MIN(days_since_event_at) OVER (PARTITION BY event_type) AS event_type_min_days_since_event_at,
        days_since_event_at - LAG(days_since_event_at) OVER (PARTITION BY event_type ORDER BY event_at_week DESC, id) AS days_since_event_at_change,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type,
        SIZE(COLLECT_SET(actor) OVER w_ticket_id_event_type) AS distinct_actor_in_ticket_id_event_type,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_event_at_so_far,
        MAX(days_since_event_at) OVER (PARTITION BY ticket_id ORDER BY days_since_event_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_event_at_max_3_prior
    FROM scoped
    WINDOW w_ticket_id_event_type AS (PARTITION BY ticket_id, event_type), w_event_type AS (PARTITION BY event_type)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_event_at_week,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        ROUND(event_at_week_cume_dist * 1.1, 2) AS event_at_week_cume_dist_adj
    FROM with_windows
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(event_at, 2) OVER (PARTITION BY MONTH(event_at), event_at_week_decile ORDER BY hours_event_at_to_event_at_week, id) AS next2_event_at,
        ROUND(event_at_week_cume_dist - AVG(event_at_week_cume_dist) OVER (PARTITION BY YEAR(event_at_week), MONTH(event_at)), 2) AS event_at_week_cume_dist_vs_event_at_week_year_ev,
        COUNT(*) OVER (PARTITION BY YEAR(event_at_week), MONTH(event_at)) AS rows_in_event_at_week_year_event_at_month,
        ROUND(rows_in_event_type - AVG(rows_in_event_type) OVER w_actor_distinct_actor_in_tick, 2) AS rows_in_event_type_vs_actor_distinct_actor_in_ti
    FROM annotated
    WINDOW w_actor_distinct_actor_in_tick AS (PARTITION BY actor, distinct_actor_in_ticket_id_event_type)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ROUND(SUM(days_since_event_at_max_3_prior) / NULLIF(SUM(event_at_week_cume_dist), 0), 3) AS days_since_event_at_max_3_prior_to_event_at_week,
        ROUND(SUM(days_since_event_at_max_3_prior) / NULLIF(SUM(max_days_since_event_at_so_far), 0), 3) AS days_since_event_at_max_3_prior_to_max_days_sinc,
        ROUND(STDDEV(max_days_since_event_at_so_far), 2) AS stddev_max_days_since_event_at_so_far,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list
    FROM with_windows_2
    GROUP BY DATE_TRUNC('MONTH', event_at)
    HAVING AVG(event_type_min_days_since_event_at) > 1050
)
SELECT
    event_at_month,
    days_since_event_at_max_3_prior_to_event_at_week,
    days_since_event_at_max_3_prior_to_max_days_sinc,
    stddev_max_days_since_event_at_so_far,
    actor_list,
    DATE_FORMAT(event_at_month, 'MMM yyyy') AS event_at_month_label,
    TO_DATE(event_at_month) AS event_at_month_date
FROM grouped
ORDER BY event_at_month_date NULLS LAST
