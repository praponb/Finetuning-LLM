WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix,
        CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
      AND ticket_id >= 26
),
tagged AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(actor)) AS actor_len
    FROM filtered
    WHERE event_at >= TIMESTAMP '2025-02-06 11:00:00'
),
event_type_group_stats AS (
    -- Median actor_len per event_type_group
    SELECT event_type_group, PERCENTILE_APPROX(actor_len, 0.5) AS median_actor_len
    FROM tagged
    GROUP BY event_type_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_actor_len,
        ROUND(s.actor_len - g.median_actor_len, 2) AS actor_len_minus_median
    FROM tagged s
    JOIN event_type_group_stats g ON g.event_type_group = s.event_type_group
    WHERE s.actor_len >= g.median_actor_len
),
rollup_base AS (
    -- Group-level totals
    SELECT
        actor,
        COUNT(*) AS cnt,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM outliers
    GROUP BY actor
    HAVING COUNT(*) >= 2
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(cnt - 6) AS cnt_distance,
        CONCAT(actor, '#', CAST(distinct_event_types AS STRING)) AS actor_ref,
        ROUND(distinct_event_types / NULLIF(cnt, 0), 3) AS distinct_event_types_per_cnt
    FROM rollup_base
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        distinct_event_types - LAG(distinct_event_types) OVER (PARTITION BY cnt, actor ORDER BY distinct_event_types_per_cnt, actor) AS distinct_event_types_change,
        LAST_VALUE(distinct_event_types_per_cnt) OVER (PARTITION BY distinct_event_types, cnt ORDER BY cnt DESC NULLS FIRST, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_event_types_per_cnt
    FROM tagged_2
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_trends
    WHERE distinct_event_types > (SELECT MIN(distinct_event_types) FROM with_trends)
)
-- Final projection
SELECT
    actor,
    cnt,
    distinct_event_types,
    distinct_event_types_per_cnt,
    last_distinct_event_types_per_cnt
FROM without_matches
ORDER BY last_distinct_event_types_per_cnt DESC NULLS FIRST, actor DESC
LIMIT 20
