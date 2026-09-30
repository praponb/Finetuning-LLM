WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id >= 5
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM filtered_events
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor_group,
        actor_slug,
        LAG(actor_len) OVER w_actor_actor_group_by_actor_l AS prev_actor_len,
        ROUND(CUME_DIST() OVER w_actor_group_by_actor_len, 3) AS actor_len_cume_dist,
        COUNT(*) OVER (PARTITION BY actor_len) AS rows_in_actor_len,
        MIN(actor_len) OVER (PARTITION BY actor_len) AS actor_len_min_actor_len,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER (PARTITION BY actor_len), 0), 4) AS actor_len_share_of_actor_len
    FROM scored
    WINDOW w_actor_actor_group_by_actor_l AS (PARTITION BY actor, actor_group ORDER BY actor_len DESC, id), w_actor_group_by_actor_len AS (PARTITION BY actor_group ORDER BY actor_len DESC, id)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        actor_group,
        prev_actor_len,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at,
        SUM(actor_len_cume_dist) AS total_actor_len_cume_dist,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    WHERE rows_in_actor_len > 3
    GROUP BY actor_group, prev_actor_len
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE prev_actor_len > (SELECT MIN(prev_actor_len) FROM rollup_base)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        actor_group,
        prev_actor_len,
        first_event_at,
        last_event_at,
        total_actor_len_cume_dist,
        MIN(distinct_ids) OVER (PARTITION BY actor_group) AS actor_group_min_distinct_ids,
        ROW_NUMBER() OVER (PARTITION BY prev_actor_len, distinct_ids ORDER BY total_actor_len_cume_dist ASC, actor_group, prev_actor_len) AS total_actor_len_cume_dist_seq,
        MIN(distinct_ids) OVER (PARTITION BY prev_actor_len, distinct_ids ORDER BY total_actor_len_cume_dist ASC, actor_group, prev_actor_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_distinct_ids_so_far,
        ROUND(AVG(distinct_ids) OVER (ORDER BY last_event_at DESC, actor_group, prev_actor_len ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_ids_avg_2_prior
    FROM qualified
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(min_distinct_ids_so_far) OVER (ORDER BY distinct_ids_avg_2_prior NULLS LAST, actor_group, prev_actor_len ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_distinct_ids_so_far,
        RANK() OVER (PARTITION BY actor_group ORDER BY actor_group_min_distinct_ids) AS actor_group_min_distinct_ids_rank,
        ROUND(AVG(prev_actor_len) OVER w_prev_actor_len_actor_group, 2) AS prev_actor_len_actor_group_avg_prev_actor_len,
        ROUND(AVG(min_distinct_ids_so_far) OVER (PARTITION BY prev_actor_len, actor_group), 2) AS prev_actor_len_actor_group_avg_min_distinct_ids_,
        ROW_NUMBER() OVER (PARTITION BY actor_group ORDER BY actor_group_min_distinct_ids, actor_group, prev_actor_len) AS rn
    FROM windowed
    WINDOW w_prev_actor_len_actor_group AS (PARTITION BY prev_actor_len, actor_group)
)
SELECT
    actor_group,
    prev_actor_len,
    first_event_at,
    total_actor_len_cume_dist,
    total_actor_len_cume_dist_seq,
    min_distinct_ids_so_far,
    distinct_ids_avg_2_prior,
    actor_group_min_distinct_ids_rank,
    prev_actor_len_actor_group_avg_prev_actor_len,
    prev_actor_len_actor_group_avg_min_distinct_ids_,
    ROUND(distinct_ids_avg_2_prior * 1.07, 2) AS distinct_ids_avg_2_prior_adj,
    ROUND(distinct_ids_avg_2_prior * 1.1, 2) AS distinct_ids_avg_2_prior_adj_2
FROM metrics
ORDER BY distinct_ids_avg_2_prior_adj_2
