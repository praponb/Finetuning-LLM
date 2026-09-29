WITH recent_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 22
      AND event_type <> 'assigned'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id) AS event_at_dense_rank,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(3) OVER w_event_at_to_date_by_event_at AS event_at_tercile,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id) AS prev2_event_at,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id) AS event_at_dense_rank_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist_2,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER w_all_by_event_at AS seq_no_2,
        NTILE(4) OVER w_event_at_to_date_by_event_at AS event_at_quartile,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM recent_events
    WINDOW w_event_at_to_date_by_event_at AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE event_at_dense_rank = 1
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_dense_rank,
        minutes_since_prev_event_at,
        event_at_tercile,
        prev2_event_at,
        event_at_cume_dist,
        event_at_cume_dist_2,
        last_event_at,
        seq_no_2,
        event_at_quartile,
        first_event_at,
        second_event_at,
        MIN(event_at_quartile) OVER (PARTITION BY MONTH(first_event_at)) AS first_event_at_month_min_event_at_quartile,
        ROUND(AVG(minutes_since_prev_event_at) OVER (PARTITION BY event_at_quartile ORDER BY last_event_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS minutes_since_prev_event_at_avg_2_prior,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at_quartile, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        LAG(seq_no_2) OVER (PARTITION BY event_at_quartile ORDER BY last_event_at DESC, id) AS prev_seq_no_2
    FROM top_per_group
),
rollup_base AS (
    -- Summarise per group
    SELECT
        TO_DATE(second_event_at) AS second_event_at_day,
        MAX(second_event_at) AS last_second_event_at,
        PERCENTILE_APPROX(first_event_at_month_min_event_at_quartile, 0.5) AS median_first_event_at_month_min_event_at_quartil
    FROM ranked_2
    WHERE minutes_since_prev_event_at_2 > 2000.0
    GROUP BY TO_DATE(second_event_at)
)
SELECT
    second_event_at_day,
    last_second_event_at,
    median_first_event_at_month_min_event_at_quartil,
    DATE_TRUNC('MONTH', second_event_at_day) AS second_event_at_day_month
FROM rollup_base
ORDER BY last_second_event_at DESC
