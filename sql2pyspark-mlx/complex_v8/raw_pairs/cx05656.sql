WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
      AND actor = 'agent'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        actor,
        actor_clean,
        event_type_or_unknown,
        ROUND(PERCENT_RANK() OVER w_event_type_or_unknown_by_eve, 3) AS event_at_percent_rank,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        LAG(event_at, 2) OVER w_all_by_event_at_2 AS prev2_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank_3,
        LEAD(event_at, 2) OVER (ORDER BY event_at DESC, id) AS next2_event_at,
        DENSE_RANK() OVER (ORDER BY event_at, id) AS event_at_dense_rank,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS next_event_at
    FROM valid_events
    WINDOW w_event_type_or_unknown_by_eve AS (PARTITION BY event_type_or_unknown ORDER BY event_at), w_all_by_event_at AS (ORDER BY event_at, id), w_all_by_event_at_ties AS (ORDER BY event_at), w_all_by_event_at_2 AS (ORDER BY event_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_trends
    WHERE event_at_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        actor,
        event_type_or_unknown,
        prev_event_at,
        first_event_at,
        rn,
        event_at_percent_rank_2,
        prev2_event_at,
        event_at_percent_rank_3,
        event_at_dense_rank,
        next_event_at,
        SIZE(COLLECT_SET(event_type_or_unknown) OVER w_next_event_at_month) AS distinct_event_type_or_unknown_in_next_event_at_,
        RANK() OVER (PARTITION BY event_at_rank, event_type_or_unknown ORDER BY event_at_percent_rank, id) AS event_at_percent_rank_rank,
        LAST_VALUE(event_at_cume_dist) IGNORE NULLS OVER (PARTITION BY actor, MONTH(next2_event_at) ORDER BY first_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_cume_dist,
        ROUND(event_at_percent_rank / NULLIF(SUM(event_at_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at)), 0), 4) AS event_at_percent_rank_share_of_next_event_at_mon,
        ROUND(AVG(rn) OVER (PARTITION BY event_type_or_unknown ORDER BY event_at_percent_rank_2 DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS rn_avg_1_prior,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at)) AS rows_in_next_event_at_month
    FROM top_rows
    WINDOW w_next_event_at_month AS (PARTITION BY DATE_TRUNC('MONTH', next_event_at))
),
totals AS (
    -- Summarise per group
    SELECT
        TO_DATE(prev_event_at) AS prev_event_at_day,
        event_type_or_unknown,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        ROUND(100 * COUNT(CASE WHEN actor NOT IN ('customer', 'system') THEN 1 END) / COUNT(*), 1) AS pct_actor_not_in_customer_system,
        COUNT(*) AS cnt
    FROM sequenced
    GROUP BY TO_DATE(prev_event_at), event_type_or_unknown
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        prev_event_at_day,
        event_type_or_unknown,
        actor_list,
        cnt,
        LEAD(cnt) OVER w_event_type_or_unknown_by_pct AS next_cnt,
        COUNT(*) OVER (PARTITION BY cnt, event_type_or_unknown) AS rows_in_cnt_event_type_or_unknown,
        LEAD(cnt, 2) OVER (PARTITION BY event_type_or_unknown ORDER BY pct_actor_not_in_customer_system DESC, prev_event_at_day, event_type_or_unknown) AS next2_cnt,
        DENSE_RANK() OVER (PARTITION BY event_type_or_unknown ORDER BY pct_actor_not_in_customer_system, prev_event_at_day, event_type_or_unknown) AS pct_actor_not_in_customer_system_dense_rank
    FROM totals
    WINDOW w_event_type_or_unknown_by_pct AS (PARTITION BY event_type_or_unknown ORDER BY pct_actor_not_in_customer_system DESC, prev_event_at_day, event_type_or_unknown)
)
-- Final projection
SELECT
    prev_event_at_day,
    event_type_or_unknown,
    actor_list,
    cnt,
    rows_in_cnt_event_type_or_unknown,
    next2_cnt,
    pct_actor_not_in_customer_system_dense_rank,
    ROUND(next_cnt / NULLIF(next2_cnt, 0), 3) AS next_cnt_per_next2_cnt,
    SUBSTRING(TRIM(event_type_or_unknown), 1, 3) AS event_type_or_unknown_prefix
FROM ranked
ORDER BY pct_actor_not_in_customer_system_dense_rank
