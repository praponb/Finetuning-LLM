WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-01-25 07:00:00'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor_group,
        event_type_actor_label,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_group_actor_by_event_a)) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(10) OVER (PARTITION BY actor_group, event_type ORDER BY event_at ASC, id) AS event_at_decile,
        LAG(event_at) OVER w_event_type_by_event_at AS prev_event_at,
        NTILE(5) OVER (PARTITION BY actor_group, actor ORDER BY event_at ASC NULLS LAST, id) AS event_at_quintile,
        NTILE(4) OVER w_event_type_by_event_at AS event_at_quartile,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id) AS prev_event_at_2,
        NTILE(5) OVER w_all_by_event_at AS event_at_quintile_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor_group, actor ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        NTILE(4) OVER (PARTITION BY actor_group, actor ORDER BY event_at ASC NULLS LAST, id) AS event_at_quartile_2,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST) AS event_at_dense_rank
    FROM filtered
    WINDOW w_actor_group_actor_by_event_a AS (PARTITION BY actor_group, actor ORDER BY event_at ASC NULLS LAST, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor_group,
        event_type_actor_label,
        minutes_since_prev_event_at,
        prev_event_at,
        event_at_quintile,
        event_at_quartile,
        prev_event_at_2,
        event_at_cume_dist,
        event_at_quartile_2,
        rn,
        event_at_dense_rank,
        DATE_FORMAT(prev_event_at_2, 'MMM yyyy') AS prev_event_at_2_label
    FROM with_trends
    WHERE event_at_quintile_2 < 7
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY event_at_dense_rank ORDER BY event_at_quartile_2, id) AS event_at_quartile_2_tercile,
        ROUND(AVG(event_at_cume_dist) OVER w_prev_event_at_month_event_at, 2) AS prev_event_at_month_event_at_quintile_avg_event_,
        ROUND(event_at_quartile - AVG(event_at_quartile) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_event_at), event_at_quintile), 2) AS event_at_quartile_vs_prev_event_at_month_event_a,
        MIN(minutes_since_prev_event_at) OVER w_prev_event_at_month_event_at AS prev_event_at_month_event_at_quintile_min_minute,
        NTH_VALUE(event_at_quintile, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_event_at) ORDER BY prev_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_quintile
    FROM annotated
    WINDOW w_prev_event_at_month_event_at AS (PARTITION BY DATE_TRUNC('MONTH', prev_event_at), event_at_quintile)
)
-- Final projection
SELECT
    id,
    event_type,
    event_type_actor_label,
    prev_event_at,
    event_at_quintile,
    event_at_quartile,
    prev_event_at_2,
    event_at_quartile_2,
    rn,
    event_at_dense_rank,
    prev_event_at_2_label,
    prev_event_at_month_event_at_quintile_avg_event_,
    event_at_quartile_vs_prev_event_at_month_event_a,
    second_event_at_quintile,
    DATE_FORMAT(prev_event_at_2, 'MMM yyyy') AS prev_event_at_2_label_2
FROM sequenced
ORDER BY event_at_quartile_vs_prev_event_at_month_event_a, prev_event_at_2_label NULLS LAST
