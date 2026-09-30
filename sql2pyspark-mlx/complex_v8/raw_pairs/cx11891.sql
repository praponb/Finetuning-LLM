WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(event_at) OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at, id) AS prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at, 2) OVER (PARTITION BY ticket_priority, ticket_id ORDER BY event_at ASC, id) AS prev2_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER w_event_type_actor_by_event_at, 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_priority, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS next_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at, id), 3) AS event_at_percent_rank_2
    FROM with_details_ticket
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(event_at_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', last_event_at) ORDER BY first_known_event_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_at_percent_rank_max_7,
        ROUND(event_at_percent_rank_2 - AVG(event_at_percent_rank_2) OVER w_prev2_event_at_year_event_at, 2) AS event_at_percent_rank_2_vs_prev2_event_at_year_e
    FROM ranked
    WINDOW w_prev2_event_at_year_event_at AS (PARTITION BY YEAR(prev2_event_at), YEAR(event_at))
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        DATE_TRUNC('MONTH', last_event_at) AS last_event_at_month,
        DATE_TRUNC('MONTH', last_known_event_at) AS last_known_event_at_month,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(AVG(event_at_percent_rank_2), 2) AS avg_event_at_percent_rank_2,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM sequenced
    WHERE event_at_percent_rank_2_vs_prev2_event_at_year_e <= 0.2
    GROUP BY DATE_TRUNC('MONTH', event_at), DATE_TRUNC('MONTH', last_event_at), DATE_TRUNC('MONTH', last_known_event_at)
    HAVING MAX(event_at_percent_rank_2_vs_prev2_event_at_year_e) > -0.8
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(avg_event_at_percent_rank_2) OVER (PARTITION BY distinct_event_types, MONTH(last_known_event_at_month)), 2) AS distinct_event_types_last_known_event_at_month_m,
        NTH_VALUE(last_known_event_at_month, 2) OVER (PARTITION BY distinct_event_types ORDER BY distinct_event_types, event_at_month, last_event_at_month, last_known_event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_known_event_at_month,
        COUNT(*) OVER w_distinct_event_types_last_kn AS rows_in_distinct_event_types_last_known_event_at
    FROM totals
    WINDOW w_distinct_event_types_last_kn AS (PARTITION BY distinct_event_types, MONTH(last_known_event_at_month))
)
-- Final projection
SELECT
    event_at_month,
    last_known_event_at_month,
    actor_list,
    avg_event_at_percent_rank_2,
    distinct_event_types,
    distinct_event_types_last_known_event_at_month_m,
    second_last_known_event_at_month,
    rows_in_distinct_event_types_last_known_event_at,
    CASE WHEN distinct_event_types >= 9 THEN 'A' WHEN distinct_event_types >= 3 THEN 'B' ELSE 'C' END AS distinct_event_types_tier,
    CASE WHEN distinct_event_types >= 8 THEN 'large' WHEN distinct_event_types >= 4 THEN 'medium' ELSE 'small' END AS distinct_event_types_tier_2
FROM metrics
ORDER BY rows_in_distinct_event_types_last_known_event_at
