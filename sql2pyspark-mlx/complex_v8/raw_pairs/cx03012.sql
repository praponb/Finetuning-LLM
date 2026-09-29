WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND ticket_id <= 10
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at) AS event_at_rank,
        NTILE(3) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS event_at_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        NTILE(5) OVER w_event_type_actor_by_event_at AS event_at_quintile,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id) AS prev2_event_at,
        LAG(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at, id) AS seq_no,
        LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS prev_event_at_2,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC NULLS FIRST, id) AS next_event_at,
        LEAD(event_at, 2) OVER w_event_type_actor_by_event_at AS next2_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC), 3) AS event_at_cume_dist_2
    FROM filtered_events
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(seq_no) OVER (PARTITION BY event_at_rank) AS event_at_rank_sum_seq_no,
        ROUND(AVG(seq_no) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_event_at) ORDER BY seq_no DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS seq_no_avg_4,
        ROUND((UNIX_TIMESTAMP(prev2_event_at) - UNIX_TIMESTAMP(LAG(prev2_event_at) OVER (PARTITION BY YEAR(prev_event_at_2), DATE_TRUNC('MONTH', next_event_at) ORDER BY next2_event_at, id))) / 60, 1) AS minutes_since_prev_prev2_event_at,
        ROUND(event_at_tercile / NULLIF(SUM(event_at_tercile) OVER (PARTITION BY event_at_rank), 0), 4) AS event_at_tercile_share_of_event_at_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY YEAR(prev_event_at_2), DATE_TRUNC('MONTH', next_event_at) ORDER BY next2_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM metrics
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', prev2_event_at) AS prev2_event_at_month,
        YEAR(prev_event_at) AS prev_event_at_year,
        PERCENTILE_APPROX(event_at_tercile_share_of_event_at_rank, 0.5) AS median_event_at_tercile_share_of_event_at_rank,
        COUNT(*) AS cnt,
        SUM(seq_no_avg_4) FILTER (WHERE actor <> 'customer') AS seq_no_avg_4_actor_customer,
        COUNT(*) FILTER (WHERE actor IN ('system', 'customer')) AS n_actor_in_system_customer,
        ROUND(100 * COUNT(CASE WHEN actor IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_actor_is_not_null
    FROM with_trends
    GROUP BY DATE_TRUNC('MONTH', prev2_event_at), YEAR(prev_event_at)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        prev_event_at_year - LAG(prev_event_at_year) OVER (PARTITION BY cnt ORDER BY cnt DESC, prev2_event_at_month, prev_event_at_year) AS prev_event_at_year_change,
        COUNT(*) OVER w_cnt_n_actor_in_system_custom AS rows_in_cnt_n_actor_in_system_customer,
        SUM(seq_no_avg_4_actor_customer) OVER (PARTITION BY cnt, n_actor_in_system_customer) AS cnt_n_actor_in_system_customer_sum_seq_no_avg_4_
    FROM per_group
    WINDOW w_cnt_n_actor_in_system_custom AS (PARTITION BY cnt, n_actor_in_system_customer)
)
SELECT DISTINCT prev2_event_at_month
FROM with_windows
