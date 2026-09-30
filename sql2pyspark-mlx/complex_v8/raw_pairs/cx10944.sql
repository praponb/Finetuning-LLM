WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND MONTH(event_at) IN (1, 2)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(5) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_quintile,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS event_at_rank,
        NTILE(4) OVER (PARTITION BY YEAR(event_at), event_type ORDER BY event_at DESC, id) AS event_at_quartile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at, id), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at) AS event_at_rank_2,
        LEAD(event_at) OVER w_actor_or_unknown_ticket_id_b AS next_event_at_2
    FROM prepared_events
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at), w_actor_or_unknown_ticket_id_b AS (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE event_at_rank_2 <= 5
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        next_event_at,
        event_at_quintile,
        prev_event_at,
        event_at_cume_dist,
        event_at_rank,
        event_at_quartile,
        second_event_at,
        event_at_percent_rank,
        first_event_at,
        event_at_rank_2,
        next_event_at_2,
        LAG(next_event_at, 2) OVER (ORDER BY event_at_cume_dist DESC, id) AS prev2_next_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_cume_dist DESC, id), 3) AS event_at_cume_dist_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(second_event_at), event_at_rank_2 ORDER BY next_event_at NULLS LAST, id) AS rn,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at))) AS distinct_actor_in_next_event_at_month,
        ROW_NUMBER() OVER (PARTITION BY event_at_rank, event_at_quartile ORDER BY second_event_at DESC NULLS FIRST, id) AS second_event_at_seq,
        SUM(event_at_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at)) AS next_event_at_month_sum_event_at_cume_dist
    FROM top_per_group
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(next_event_at_2, 'yyyy-MM') AS next_event_at_2_ym,
        DATE_TRUNC('MONTH', prev_event_at) AS prev_event_at_month,
        MAX(next_event_at_month_sum_event_at_cume_dist) AS max_next_event_at_month_sum_event_at_cume_dist,
        MIN(event_at_rank) AS min_event_at_rank
    FROM with_trends
    GROUP BY DATE_FORMAT(next_event_at_2, 'yyyy-MM'), DATE_TRUNC('MONTH', prev_event_at)
    HAVING MAX(event_at_quartile) > 2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        next_event_at_2_ym,
        prev_event_at_month,
        max_next_event_at_month_sum_event_at_cume_dist,
        ROUND(min_event_at_rank / NULLIF(SUM(min_event_at_rank) OVER (PARTITION BY min_event_at_rank), 0), 4) AS min_event_at_rank_share_of_min_event_at_rank,
        LAST_VALUE(prev_event_at_month) IGNORE NULLS OVER (PARTITION BY MONTH(prev_event_at_month) ORDER BY max_next_event_at_month_sum_event_at_cume_dist, next_event_at_2_ym, prev_event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_event_at_month,
        RANK() OVER (PARTITION BY MONTH(prev_event_at_month) ORDER BY max_next_event_at_month_sum_event_at_cume_dist) AS max_next_event_at_month_sum_event_at_cume_dist_r,
        MAX(min_event_at_rank) OVER (PARTITION BY MONTH(prev_event_at_month) ORDER BY max_next_event_at_month_sum_event_at_cume_dist, next_event_at_2_ym, prev_event_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_min_event_at_rank_so_far
    FROM rollup_base
)
SELECT
    next_event_at_2_ym,
    min_event_at_rank_share_of_min_event_at_rank,
    last_known_prev_event_at_month,
    max_next_event_at_month_sum_event_at_cume_dist_r,
    max_min_event_at_rank_so_far,
    SUBSTRING(TRIM(next_event_at_2_ym), 1, 4) AS next_event_at_2_ym_prefix,
    LENGTH(TRIM(next_event_at_2_ym)) AS next_event_at_2_ym_len
FROM ranked
WHERE last_known_prev_event_at_month >= TIMESTAMP '2025-01-13 09:00:00'
ORDER BY last_known_prev_event_at_month
