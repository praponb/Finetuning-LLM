WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_type = 'replied'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER w_event_at_month_by_event_at AS event_at_tercile,
        NTILE(5) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quintile,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_dense_rank,
        LEAD(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id) AS next_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id) AS event_at_dense_rank_2,
        ROW_NUMBER() OVER w_event_type_by_event_at AS event_at_seq,
        NTILE(4) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC) AS event_at_dense_rank_3,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_dense_rank_4,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2
    FROM filtered_events
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_all_by_event_at AS (ORDER BY event_at DESC NULLS FIRST, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label,
        event_at_tercile,
        event_at_quintile,
        event_at_dense_rank,
        next_event_at,
        event_at_dense_rank_2,
        event_at_quartile,
        event_at_dense_rank_4,
        event_at_cume_dist,
        last_event_at_2,
        ROUND(event_at_dense_rank_3 / NULLIF(event_at_quartile, 0), 3) AS event_at_dense_rank_3_per_event_at_quartile
    FROM windowed
),
windowed_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_actor_label,
        event_at_tercile,
        event_at_quintile,
        event_at_dense_rank,
        event_at_dense_rank_2,
        event_at_quartile,
        event_at_dense_rank_4,
        last_event_at_2,
        ROUND(event_at_quintile - AVG(event_at_quintile) OVER (PARTITION BY event_at_dense_rank_2, event_at_dense_rank_4), 2) AS event_at_quintile_vs_event_at_dense_rank_2_event,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_at_dense_rank_4, TO_DATE(last_event_at_2))) AS distinct_ticket_id_in_event_at_dense_rank_4_last,
        MIN(event_at_quintile) OVER w_event_at_dense_rank_4_last_e AS event_at_dense_rank_4_last_event_at_2_to_date_mi,
        COUNT(*) OVER (PARTITION BY event_at_dense_rank_2, event_at_dense_rank_4) AS rows_in_event_at_dense_rank_2_event_at_dense_ran,
        COUNT(*) OVER (PARTITION BY event_at_dense_rank_4, TO_DATE(last_event_at_2)) AS rows_in_event_at_dense_rank_4_last_event_at_2_to,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_dense_rank_2, event_at_dense_rank_4)) AS distinct_actor_in_event_at_dense_rank_2_event_at
    FROM annotated
    WINDOW w_event_at_dense_rank_4_last_e AS (PARTITION BY event_at_dense_rank_4, TO_DATE(last_event_at_2))
),
per_group AS (
    -- Summarise per group
    SELECT
        event_at_dense_rank_4,
        ROUND(AVG(event_at_quintile_vs_event_at_dense_rank_2_event), 2) AS avg_event_at_quintile_vs_event_at_dense_rank_2_e,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed_2
    GROUP BY event_at_dense_rank_4
    HAVING MAX(event_at_quartile) > 3
)
SELECT
    event_at_dense_rank_4,
    distinct_ids,
    LEAST(distinct_ids, event_at_dense_rank_4) AS least_distinct_ids_event_at_dense_rank_4,
    ABS(event_at_dense_rank_4 - 7) AS event_at_dense_rank_4_distance
FROM per_group
ORDER BY least_distinct_ids_event_at_dense_rank_4 DESC NULLS FIRST, event_at_dense_rank_4
LIMIT 5
