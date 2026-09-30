WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE (MONTH(event_at) IN (1, 3) OR event_type IS NOT NULL)
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_at_plus_30m,
        s.event_type_actor_label,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM scoped s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(event_at) AS event_at_year,
        event_event_type,
        MAX(event_at) AS last_event_at,
        SUM(CASE WHEN event_event_type IN ('assigned', 'created', 'resolved') THEN 1 ELSE 0 END) AS n_event_event_type_in_assigned,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list,
        ARRAY_SORT(COLLECT_SET(event_event_type)) AS event_event_type_list,
        COUNT(*) FILTER (WHERE event_event_type IN ('resolved', 'created')) AS n_event_event_type_in_resolved
    FROM with_children
    GROUP BY YEAR(event_at), event_event_type
    HAVING COUNT(*) >= 2
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(event_at_year) OVER w_event_at_year, 2) AS event_at_year_avg_event_at_year,
        RANK() OVER (PARTITION BY event_event_type, n_event_event_type_in_resolved ORDER BY n_event_event_type_in_assigned ASC, event_at_year, event_event_type) AS n_event_event_type_in_assigned_rank
    FROM totals
    WINDOW w_event_at_year AS (PARTITION BY event_at_year)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        event_at_year - cnt AS event_at_year_minus_cnt,
        GREATEST(event_at_year, cnt) AS greatest_event_at_year_cnt
    FROM ranked
    WHERE event_event_type IN ('resolved', 'escalated', 'replied')
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        event_at_year,
        event_event_type,
        n_event_event_type_in_assigned,
        cnt,
        event_event_type_list,
        n_event_event_type_in_resolved,
        event_at_year_avg_event_at_year,
        ROUND(event_at_year - AVG(event_at_year) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at)), 2) AS event_at_year_vs_last_event_at_month_avg,
        NTH_VALUE(event_at_year_minus_cnt, 2) OVER (PARTITION BY n_event_event_type_in_assigned ORDER BY n_event_event_type_in_resolved ASC, event_at_year, event_event_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_year_minus_cnt,
        ROUND(PERCENT_RANK() OVER (PARTITION BY n_event_event_type_in_assigned ORDER BY n_event_event_type_in_resolved ASC), 3) AS n_event_event_type_in_resolved_percent_rank,
        SIZE(COLLECT_SET(event_event_type) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at))) AS distinct_event_event_type_in_last_event_at_month,
        LEAD(greatest_event_at_year_cnt) OVER w_n_event_event_type_in_assign_3 AS next_greatest_event_at_year_cnt,
        ROUND(greatest_event_at_year_cnt - AVG(greatest_event_at_year_cnt) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at)), 2) AS greatest_event_at_year_cnt_vs_last_event_at_mont
    FROM tagged
    WINDOW w_n_event_event_type_in_assign_3 AS (PARTITION BY n_event_event_type_in_assigned ORDER BY n_event_event_type_in_resolved ASC, event_at_year, event_event_type)
)
-- Final projection
SELECT
    event_at_year,
    event_event_type,
    event_event_type_list,
    n_event_event_type_in_resolved,
    event_at_year_avg_event_at_year,
    event_at_year_vs_last_event_at_month_avg,
    n_event_event_type_in_resolved_percent_rank,
    distinct_event_event_type_in_last_event_at_month,
    next_greatest_event_at_year_cnt,
    greatest_event_at_year_cnt_vs_last_event_at_mont,
    SUBSTRING(TRIM(event_event_type), 1, 2) AS event_event_type_prefix,
    COALESCE(event_at_year, 0) AS event_at_year_filled
FROM ranked_2
ORDER BY n_event_event_type_in_resolved DESC NULLS FIRST
