WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour
    FROM ticket_events
    WHERE actor = 'customer'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_at_hour,
        LEAD(event_at_hour) OVER w_all_by_event_at_hour AS next_event_at_hour,
        FIRST_VALUE(event_at_hour) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_hour,
        DENSE_RANK() OVER w_ticket_id_by_event_at_hour AS event_at_hour_dense_rank,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_rank,
        LEAD(event_at, 3) OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id) AS next3_event_at,
        NTILE(3) OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id) AS event_at_hour_tercile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTILE(5) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_quintile,
        FIRST_VALUE(event_at_hour) IGNORE NULLS OVER (ORDER BY event_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_hour_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (ORDER BY event_at_hour, id) AS event_at_hour_dense_rank_3,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id) AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        NTILE(4) OVER w_ticket_id_event_type_by_even_2 AS event_at_quartile
    FROM prepared
    WINDOW w_all_by_event_at_hour AS (ORDER BY event_at_hour, id), w_ticket_id_by_event_at_hour AS (PARTITION BY ticket_id ORDER BY event_at_hour, id), w_ticket_id_event_type_by_even_2 AS (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE event_at_hour_dense_rank_3 = 1
      AND second_event_at < TIMESTAMP '2025-03-08 09:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(event_at_hour_tercile, 2) OVER (PARTITION BY event_at_hour_tercile, event_at_rank ORDER BY event_at_hour_dense_rank_3 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_hour_tercile,
        SUM(event_at_hour_dense_rank_3) OVER w_ticket_id_event_at_hour_to_d AS ticket_id_event_at_hour_to_date_sum_event_at_hou,
        ROUND(AVG(event_at_hour_dense_rank) OVER (ORDER BY event_at_seq DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_hour_dense_rank_avg_4
    FROM top_per_group
    WINDOW w_ticket_id_event_at_hour_to_d AS (PARTITION BY ticket_id, TO_DATE(event_at_hour))
)
SELECT
    id,
    event_type,
    event_at_hour,
    next_event_at_hour,
    first_known_event_at_hour,
    event_at_hour_dense_rank,
    event_at_rank,
    event_at_hour_tercile,
    last_known_event_at,
    event_at_quintile,
    event_at_hour_dense_rank_3,
    event_at_seq,
    second_event_at_2,
    event_at_quartile,
    second_event_at_hour_tercile,
    ticket_id_event_at_hour_to_date_sum_event_at_hou,
    event_at_hour_dense_rank_avg_4,
    event_at_rank - event_at_hour_dense_rank AS event_at_rank_minus_event_at_hour_dense_rank,
    FLOOR(event_at_hour_tercile / 2) * 2 AS event_at_hour_tercile_bucket
FROM ranked
WHERE event_at_hour_dense_rank <= 7
ORDER BY event_at_rank_minus_event_at_hour_dense_rank DESC
