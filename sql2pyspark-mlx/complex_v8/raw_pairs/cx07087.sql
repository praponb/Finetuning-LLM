WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor = 'agent'
      AND event_at >= TIMESTAMP '2025-02-06 10:00:00'
),
grouped AS (
    -- Summarise per group
    SELECT
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        COUNT(*) AS cnt,
        COUNT(DISTINCT actor) AS distinct_actors,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM valid
    GROUP BY actor, DATE_TRUNC('MONTH', event_at)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(cnt, 2) OVER (PARTITION BY distinct_event_types ORDER BY distinct_actors DESC, actor, event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_cnt,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_event_types ORDER BY distinct_actors DESC), 3) AS distinct_actors_cume_dist
    FROM grouped
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        distinct_ids - LAG(distinct_ids) OVER (ORDER BY distinct_ids, actor, event_at_month) AS distinct_ids_change,
        ROUND(AVG(cnt) OVER (PARTITION BY distinct_event_types, DATE_TRUNC('MONTH', event_at_month)), 2) AS distinct_event_types_event_at_month_month_avg_cn,
        SUM(distinct_ids) OVER w_distinct_event_types_event_a AS distinct_event_types_event_at_month_month_sum_di,
        ROUND(PERCENT_RANK() OVER w_all_by_second_cnt, 3) AS second_cnt_percent_rank,
        LEAD(distinct_actors) OVER (ORDER BY second_cnt DESC, actor, event_at_month) AS next_distinct_actors,
        LEAD(distinct_ids, 3) OVER (ORDER BY second_cnt DESC, actor, event_at_month) AS next3_distinct_ids
    FROM metrics
    WINDOW w_distinct_event_types_event_a AS (PARTITION BY distinct_event_types, DATE_TRUNC('MONTH', event_at_month)), w_all_by_second_cnt AS (ORDER BY second_cnt DESC, actor, event_at_month)
)
-- Final projection
SELECT
    actor,
    cnt,
    distinct_actors,
    distinct_ids,
    distinct_event_types,
    second_cnt,
    distinct_actors_cume_dist,
    distinct_ids_change,
    distinct_event_types_event_at_month_month_avg_cn,
    distinct_event_types_event_at_month_month_sum_di,
    second_cnt_percent_rank,
    next_distinct_actors,
    next3_distinct_ids
FROM sequenced
ORDER BY distinct_ids DESC, second_cnt_percent_rank DESC
