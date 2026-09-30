WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        QUARTER(event_at) AS event_at_quarter
    FROM ticket_events
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC), 3) AS event_at_cume_dist_2,
        ROUND(event_at_quarter / NULLIF(SUM(event_at_quarter) OVER (PARTITION BY event_type, actor), 0), 4) AS event_at_quarter_share_of_event_type_actor,
        LAG(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS prev2_event_at,
        COUNT(*) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_type, actor)) AS distinct_ticket_id_in_event_type_actor,
        COUNT(*) OVER (PARTITION BY event_type, actor) AS rows_in_event_type_actor,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS next_event_at,
        MAX(event_at_quarter) OVER (PARTITION BY event_type, actor) AS event_type_actor_max_event_at_quarter,
        LAST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        SUM(event_at_quarter) OVER (ORDER BY event_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_quarter_sum_6_prior,
        ROUND(AVG(event_at_quarter) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_quarter_avg_3
    FROM filtered_events
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at ASC), w_event_type_actor AS (PARTITION BY event_type, actor), w_all_by_event_at AS (ORDER BY event_at ASC, id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        actor,
        event_at_quarter,
        event_at_cume_dist,
        event_at_cume_dist_2,
        event_at_quarter_share_of_event_type_actor,
        prev2_event_at,
        running_count,
        prev_event_at,
        event_type_actor_max_event_at_quarter,
        last_event_at,
        ROUND(event_at_cume_dist_2 / NULLIF(event_at_quarter_avg_3, 0), 3) AS event_at_cume_dist_2_per_event_at_quarter_avg_3,
        ABS(rows_in_event_type_actor - 8) AS rows_in_event_type_actor_distance
    FROM with_trends
    WHERE event_at_cume_dist >= 0.3
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY MONTH(prev_event_at))) AS distinct_actor_in_prev_event_at_month,
        NTH_VALUE(prev_event_at, 2) OVER (ORDER BY rows_in_event_type_actor_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_type_actor_max_event_at_quarter ASC), 3) AS event_type_actor_max_event_at_quarter_cume_dist,
        ROUND(AVG(event_at_cume_dist_2) OVER w_prev_event_at_month, 2) AS prev_event_at_month_avg_event_at_cume_dist_2,
        ROW_NUMBER() OVER w_all_by_event_at_cume_dist_2 AS event_at_cume_dist_2_seq
    FROM enriched
    WINDOW w_prev_event_at_month AS (PARTITION BY MONTH(prev_event_at)), w_all_by_event_at_cume_dist_2 AS (ORDER BY event_at_cume_dist_2 DESC, id)
)
SELECT
    id,
    event_at,
    actor,
    event_at_quarter,
    event_at_quarter_share_of_event_type_actor,
    prev2_event_at,
    prev_event_at,
    event_type_actor_max_event_at_quarter,
    last_event_at,
    rows_in_event_type_actor_distance,
    distinct_actor_in_prev_event_at_month,
    second_prev_event_at,
    event_type_actor_max_event_at_quarter_cume_dist,
    event_at_cume_dist_2_seq,
    DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
    ROUND((UNIX_TIMESTAMP(prev_event_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_prev_event_at
FROM with_windows
ORDER BY event_type_actor_max_event_at_quarter
