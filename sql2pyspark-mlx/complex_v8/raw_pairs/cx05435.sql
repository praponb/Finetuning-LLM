WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE actor NOT IN ('agent', 'system')
      AND ticket_id IS NOT NULL
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        event_type,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        ROUND(100 * COUNT(CASE WHEN event_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_event_type_is_not_null,
        ROUND(100 * COUNT(CASE WHEN event_type = 'resolved' THEN 1 END) / COUNT(*), 1) AS pct_event_type_resolved,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE event_type NOT IN ('escalated', 'reopened')) AS n_event_type_not_in_escalated
    FROM prepared
    WHERE actor_or_unknown <> 'customer'
    GROUP BY DATE_TRUNC('MONTH', event_at), event_type
    HAVING COUNT(*) >= 2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(event_type)) AS event_type_clean,
        LEAST(distinct_event_types, row_count) AS least_distinct_event_types_row_count,
        CASE WHEN distinct_event_types >= 7 THEN 'A' WHEN distinct_event_types >= 5 THEN 'B' ELSE 'C' END AS distinct_event_types_tier
    FROM totals
    WHERE row_count BETWEEN 4 AND 8
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(distinct_event_types, pct_event_type_resolved) AS greatest_distinct_event_types_pct_event_type_res,
        CONCAT(event_type, '#', CAST(row_count AS STRING)) AS event_type_ref,
        DAYOFWEEK(event_at_month) AS event_at_month_dayofweek
    FROM labeled
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_distinct_event_types_least_d AS pct_event_type_is_not_null_seq,
        RANK() OVER w_distinct_event_types_least_d AS pct_event_type_is_not_null_rank,
        LAG(distinct_event_types) OVER (PARTITION BY distinct_event_types, least_distinct_event_types_row_count ORDER BY pct_event_type_is_not_null, event_at_month, event_type) AS prev_distinct_event_types,
        ROUND(PERCENT_RANK() OVER w_distinct_event_types_least_d_2, 3) AS pct_event_type_is_not_null_percent_rank,
        ROUND(AVG(row_count) OVER (PARTITION BY distinct_event_types, least_distinct_event_types_row_count ORDER BY pct_event_type_is_not_null, event_at_month, event_type ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS row_count_avg_7
    FROM annotated
    WINDOW w_distinct_event_types_least_d AS (PARTITION BY distinct_event_types, least_distinct_event_types_row_count ORDER BY pct_event_type_is_not_null, event_at_month, event_type), w_distinct_event_types_least_d_2 AS (PARTITION BY distinct_event_types, least_distinct_event_types_row_count ORDER BY pct_event_type_is_not_null)
),
distinct_event_types_tier_stats AS (
    -- Avg row_count_avg_7 per distinct_event_types_tier
    SELECT distinct_event_types_tier, AVG(row_count_avg_7) AS avg_row_count_avg_7
    FROM ranked
    GROUP BY distinct_event_types_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_row_count_avg_7,
        ROUND(s.row_count_avg_7 - g.avg_row_count_avg_7, 2) AS row_count_avg_7_minus_avg
    FROM ranked s
    JOIN distinct_event_types_tier_stats g ON g.distinct_event_types_tier = s.distinct_event_types_tier
    WHERE s.row_count_avg_7 > g.avg_row_count_avg_7
)
SELECT
    row_count,
    prev_distinct_event_types,
    least_distinct_event_types_row_count,
    ROUND(STDDEV(pct_event_type_is_not_null_percent_rank), 2) AS stddev_pct_event_type_is_not_null_percent_rank,
    COUNT(*) AS n_rows
FROM compared
GROUP BY CUBE(row_count, prev_distinct_event_types, least_distinct_event_types_row_count)
ORDER BY row_count NULLS LAST, prev_distinct_event_types NULLS LAST, least_distinct_event_types_row_count NULLS LAST
