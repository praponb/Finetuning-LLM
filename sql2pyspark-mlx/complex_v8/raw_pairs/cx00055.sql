WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 10:00:00'
      AND actor = 'system'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(actor_len, 1, 0) OVER (PARTITION BY actor, ticket_id ORDER BY actor_len ASC, id) AS next_actor_len,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor_len)) AS distinct_event_type_in_actor_len,
        COUNT(*) OVER (PARTITION BY actor_len) AS rows_in_actor_len,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER w_actor_len, 0), 4) AS actor_len_share_of_actor_len
    FROM scoped_events
    WINDOW w_actor_len AS (PARTITION BY actor_len)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_len,
        days_since_event_at,
        distinct_event_type_in_actor_len,
        actor_len_share_of_actor_len,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY distinct_event_type_in_actor_len ASC, id) AS seq_no,
        ROUND(CUME_DIST() OVER (ORDER BY distinct_event_type_in_actor_len), 3) AS distinct_event_type_in_actor_len_cume_dist,
        SIZE(COLLECT_SET(event_type) OVER w_event_at_month) AS distinct_event_type_in_event_at_month,
        LAST_VALUE(rows_in_actor_len) IGNORE NULLS OVER (ORDER BY distinct_event_type_in_actor_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_actor_len,
        LAST_VALUE(actor_len) IGNORE NULLS OVER (ORDER BY distinct_event_type_in_actor_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_actor_len
    FROM ranked
    WINDOW w_event_at_month AS (PARTITION BY DATE_TRUNC('MONTH', event_at))
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(seq_no) OVER (PARTITION BY distinct_event_type_in_actor_len ORDER BY distinct_event_type_in_event_at_month ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS seq_no_avg_4,
        SIZE(COLLECT_SET(actor) OVER w_event_at_month_seq_no) AS distinct_actor_in_event_at_month_seq_no,
        MAX(seq_no) OVER (PARTITION BY MONTH(event_at), seq_no) AS event_at_month_seq_no_max_seq_no,
        DENSE_RANK() OVER (PARTITION BY actor, distinct_event_type_in_actor_len ORDER BY distinct_event_type_in_actor_len DESC) AS distinct_event_type_in_actor_len_dense_rank,
        NTH_VALUE(actor_len_share_of_actor_len, 2) OVER (PARTITION BY actor, distinct_event_type_in_actor_len ORDER BY distinct_event_type_in_actor_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len_share_of_actor_len,
        MAX(last_known_rows_in_actor_len) OVER (PARTITION BY distinct_event_type_in_actor_len ORDER BY distinct_event_type_in_event_at_month ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS last_known_rows_in_actor_len_max_2
    FROM sequenced
    WINDOW w_event_at_month_seq_no AS (PARTITION BY MONTH(event_at), seq_no)
)
-- Subtotals with ROLLUP
SELECT
    distinct_event_type_in_actor_len_dense_rank,
    distinct_event_type_in_actor_len,
    GROUPING_ID() AS gid,
    MIN(distinct_event_type_in_actor_len_dense_rank) AS min_distinct_event_type_in_actor_len_dense_rank,
    COUNT(*) FILTER (WHERE event_type NOT IN ('created', 'resolved')) AS n_event_type_not_in_created_re,
    PERCENTILE_APPROX(distinct_event_type_in_actor_len, 0.5) AS median_distinct_event_type_in_actor_len
FROM metrics
GROUP BY ROLLUP(distinct_event_type_in_actor_len_dense_rank, distinct_event_type_in_actor_len)
ORDER BY distinct_event_type_in_actor_len_dense_rank NULLS LAST, distinct_event_type_in_actor_len NULLS LAST
