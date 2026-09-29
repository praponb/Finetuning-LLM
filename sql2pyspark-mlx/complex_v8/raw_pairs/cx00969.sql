WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY days_since_event_at DESC) AS days_since_event_at_rank,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER (PARTITION BY actor_group), 2) AS days_since_event_at_vs_actor_group_avg,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS days_since_event_at_avg_3_prior,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, event_type ORDER BY days_since_event_at, id), 3) AS days_since_event_at_percent_rank,
        LAST_VALUE(days_since_event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY days_since_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(event_at) ORDER BY days_since_event_at DESC), 3) AS days_since_event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_at_to_date_by_days_sin_2)) / 60, 1) AS minutes_since_prev_event_at_2,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY actor_group), 2) AS actor_group_avg_days_since_event_at
    FROM cleaned
    WINDOW w_event_at_to_date_by_days_sin_2 AS (PARTITION BY TO_DATE(event_at) ORDER BY days_since_event_at DESC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        actor_group,
        days_since_event_at_vs_actor_group_avg,
        days_since_event_at_avg_3_prior,
        minutes_since_prev_event_at,
        days_since_event_at_percent_rank,
        last_known_days_since_event_at,
        days_since_event_at_cume_dist,
        minutes_since_prev_event_at_2,
        actor_group_avg_days_since_event_at
    FROM with_trends
    WHERE days_since_event_at_rank <= 5
),
per_group AS (
    -- Summarise per group
    SELECT
        CASE WHEN days_since_event_at >= 595 THEN 'HIGH' WHEN days_since_event_at >= 245 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_event_at_tier,
        SUM(CASE WHEN event_type = 'assigned' THEN 1 ELSE 0 END) AS n_event_type_assigned,
        COUNT(*) AS n_rows
    FROM first_rows
    WHERE days_since_event_at_avg_3_prior BETWEEN 245.0 AND 665.0
    GROUP BY 1
)
-- Final projection
SELECT
    days_since_event_at_tier,
    n_event_type_assigned,
    n_rows,
    INITCAP(TRIM(days_since_event_at_tier)) AS days_since_event_at_tier_clean
FROM per_group
WHERE n_rows BETWEEN 3 AND 9
ORDER BY n_rows, days_since_event_at_tier_clean DESC
