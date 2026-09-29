WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 10 AND 16
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at_day)) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at_day) OVER (PARTITION BY ticket_id, actor ORDER BY event_at_day DESC, id) AS next_event_at_day,
        LAST_VALUE(event_at_day) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_day,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at_day, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at_day, id), 3) AS event_at_day_cume_dist,
        NTH_VALUE(event_at_day, 2) OVER (PARTITION BY ticket_id ORDER BY event_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_day,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at_day, id) AS event_at_day_dense_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2,
        RANK() OVER (PARTITION BY MONTH(event_at_day) ORDER BY event_at DESC NULLS FIRST) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at_3,
        NTH_VALUE(event_at_day, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at_day DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_day_2,
        LEAD(event_at_day, 3) OVER (PARTITION BY MONTH(event_at_day) ORDER BY event_at DESC NULLS FIRST, id) AS next3_event_at_day,
        LEAD(event_at_day, 2) OVER w_all_by_event_at AS next2_event_at_day
    FROM cleaned
    WINDOW w_ticket_id_by_event_at_day AS (PARTITION BY ticket_id ORDER BY event_at_day, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_day,
        last_event_at,
        minutes_since_prev_event_at,
        next_event_at_day,
        last_event_at_day,
        event_at_day_cume_dist,
        second_event_at_day,
        event_at_dense_rank,
        event_at_day_dense_rank,
        last_event_at_2,
        event_at_rank,
        minutes_since_prev_event_at_3,
        second_event_at_day_2,
        next3_event_at_day,
        next2_event_at_day
    FROM metrics
    WHERE rn = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(event_at_dense_rank) OVER (ORDER BY second_event_at_day_2 DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_dense_rank_max_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY minutes_since_prev_event_at_3 ASC, id), 3) AS minutes_since_prev_event_at_3_percent_rank,
        ROUND(event_at_rank - AVG(event_at_rank) OVER (PARTITION BY MONTH(event_at_day)), 2) AS event_at_rank_vs_event_at_day_month_avg,
        NTILE(4) OVER (ORDER BY minutes_since_prev_event_at_3 ASC, id) AS minutes_since_prev_event_at_3_quartile
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_day,
    last_event_at,
    minutes_since_prev_event_at,
    next_event_at_day,
    last_event_at_day,
    event_at_day_cume_dist,
    second_event_at_day,
    event_at_dense_rank,
    event_at_day_dense_rank,
    last_event_at_2,
    event_at_rank,
    next2_event_at_day,
    event_at_dense_rank_max_3_prior,
    minutes_since_prev_event_at_3_percent_rank,
    event_at_rank_vs_event_at_day_month_avg,
    minutes_since_prev_event_at_3_quartile
FROM sequenced
ORDER BY event_at DESC, id
LIMIT 5
