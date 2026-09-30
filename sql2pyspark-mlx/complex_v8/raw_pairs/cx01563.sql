WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label_2
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-01-31 13:00:00' OR ticket_id IS NOT NULL)
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) FILTER (WHERE actor IN ('system', 'customer')) AS n_actor_in_system_customer,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN event_type <> 'replied' THEN 1 END) / COUNT(*), 1) AS pct_event_type_replied
    FROM scoped_events
    GROUP BY DATE_TRUNC('MONTH', event_at)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE pct_event_type_replied > (SELECT MIN(pct_event_type_replied) FROM per_group)
),
derived AS (
    -- Add calculated columns
    SELECT
        event_at_month,
        pct_event_type_replied,
        DATE_FORMAT(event_at_month, 'MMM yyyy') AS event_at_month_label,
        COALESCE(pct_event_type_replied, 0.0) AS pct_event_type_replied_filled
    FROM screened
    WHERE n_actor_in_system_customer >= 2
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE pct_event_type_replied_filled > (SELECT AVG(pct_event_type_replied_filled) FROM derived)
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN pct_event_type_replied >= 60.0 THEN 'HIGH' WHEN pct_event_type_replied >= 40.0 THEN 'MEDIUM' ELSE 'LOW' END AS pct_event_type_replied_tier,
        DATE_FORMAT(event_at_month, 'yyyy-MM-dd') AS event_at_month_label_2,
        DATE_TRUNC('DAY', event_at_month) AS event_at_month_day
    FROM with_activity
    WHERE HOUR(event_at_month) BETWEEN 7 AND 10
)
SELECT
    event_at_month,
    pct_event_type_replied,
    event_at_month_label,
    pct_event_type_replied_filled,
    pct_event_type_replied_tier,
    event_at_month_label_2,
    event_at_month_day,
    ABS(pct_event_type_replied - 65.0) AS pct_event_type_replied_distance,
    ABS(pct_event_type_replied - 45.0) AS pct_event_type_replied_distance_2
FROM derived_2
WHERE pct_event_type_replied_filled > 35.0
ORDER BY pct_event_type_replied_distance_2, event_at_month_label_2
