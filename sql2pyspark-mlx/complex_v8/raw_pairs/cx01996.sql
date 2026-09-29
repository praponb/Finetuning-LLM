WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
      AND actor NOT IN ('agent', 'customer')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_type_ref,
        FIRST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS next2_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER w_event_type_actor_by_event_at AS rn,
        NTILE(10) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_decile,
        NTILE(4) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_quartile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_3
    FROM recent
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        first_event_at,
        next2_event_at,
        second_event_at,
        rn,
        event_at_decile,
        event_at_quartile,
        minutes_since_prev_event_at,
        first_event_at_2,
        event_at_rank,
        second_event_at_2,
        YEAR(next2_event_at) AS next2_event_at_year,
        rn - event_at_rank AS rn_minus_event_at_rank
    FROM ranked
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER w_rn_by_first_event_at_2)) / 60, 1) AS minutes_since_prev_second_event_at,
        ROW_NUMBER() OVER (PARTITION BY event_at_decile, DATE_TRUNC('MONTH', first_event_at) ORDER BY event_at, id) AS rn_2,
        SUM(rn) OVER (PARTITION BY event_at_decile ORDER BY rn_minus_event_at_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rn,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id, rn)) AS distinct_actor_in_ticket_id_rn,
        MAX(event_at_quartile) OVER (PARTITION BY ticket_id, rn) AS ticket_id_rn_max_event_at_quartile
    FROM derived
    WINDOW w_rn_by_first_event_at_2 AS (PARTITION BY rn ORDER BY first_event_at_2, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(next2_event_at) AS next2_event_at_day,
        ROUND(AVG(rn), 2) AS avg_rn,
        COUNT(*) AS row_count,
        MAX(minutes_since_prev_second_event_at) AS max_minutes_since_prev_second_event_at,
        SUM(minutes_since_prev_second_event_at) AS total_minutes_since_prev_second_event_at
    FROM ranked_2
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY MONTH(next2_event_at_day)), 2) AS row_count_vs_next2_event_at_day_month_avg,
        ROW_NUMBER() OVER w_all_by_avg_rn AS rn
    FROM summary
    WINDOW w_all_by_avg_rn AS (ORDER BY avg_rn DESC, next2_event_at_day), w_next2_event_at_day_month AS (PARTITION BY MONTH(next2_event_at_day))
)
-- Final projection
SELECT
    next2_event_at_day,
    avg_rn,
    row_count,
    row_count_vs_next2_event_at_day_month_avg,
    rn
FROM windowed
ORDER BY row_count_vs_next2_event_at_day_month_avg DESC NULLS FIRST, row_count
