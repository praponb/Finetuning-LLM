WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-02-06 12:00:00'
      AND event_type NOT IN ('resolved', 'reopened')
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_plus_2h,
        t.priority AS ticket_priority
    FROM base_events s
    JOIN tickets t ON t.id = s.ticket_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_priority,
        LAG(event_at) OVER w_ticket_id_ticket_priority_by AS prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, MONTH(event_at) ORDER BY event_at_plus_2h, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTH_VALUE(event_at_plus_2h, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_plus_2h), MONTH(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_2h,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_plus_2h NULLS LAST), 3) AS event_at_plus_2h_percent_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at_plus_2h) - UNIX_TIMESTAMP(LAG(event_at_plus_2h) OVER (PARTITION BY ticket_id, actor ORDER BY event_at_plus_2h, id))) / 60, 1) AS minutes_since_prev_event_at_plus_2h,
        LAG(event_at_plus_2h, 2) OVER w_ticket_id_ticket_priority_by AS prev2_event_at_plus_2h,
        FIRST_VALUE(event_at_plus_2h) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at_plus_2h NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_plus_2h,
        NTH_VALUE(event_at_plus_2h, 2) OVER (PARTITION BY ticket_id, ticket_priority ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_plus_2h_3,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at_plus_2h), MONTH(event_at) ORDER BY event_at DESC, id) AS event_at_decile,
        DENSE_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at_plus_2h, id) AS event_at_plus_2h_dense_rank,
        ROUND(CUME_DIST() OVER w_ticket_id_ticket_priority_by_3, 3) AS event_at_cume_dist,
        DENSE_RANK() OVER w_ticket_id_ticket_priority_by_3 AS event_at_dense_rank
    FROM combined_details_ticket
    WINDOW w_ticket_id_ticket_priority_by AS (PARTITION BY ticket_id, ticket_priority ORDER BY event_at DESC, id), w_ticket_id_ticket_priority_by_3 AS (PARTITION BY ticket_id, ticket_priority ORDER BY event_at DESC)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_second_event_at_to_date AS rows_in_second_event_at_to_date,
        MAX(event_at_cume_dist) OVER (PARTITION BY ticket_id ORDER BY prev2_event_at_plus_2h NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_at_cume_dist_max_2_prior,
        FIRST_VALUE(second_event_at_plus_2h) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY prev2_event_at_plus_2h NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_second_event_at_plus_2h,
        ROUND(AVG(event_at_cume_dist) OVER (PARTITION BY TO_DATE(second_event_at)), 2) AS second_event_at_to_date_avg_event_at_cume_dist,
        LEAD(event_at_decile, 1, 0) OVER w_ticket_id_by_prev2_event_at_3 AS next_event_at_decile,
        ROUND(AVG(event_at_decile) OVER (PARTITION BY TO_DATE(second_event_at)), 2) AS second_event_at_to_date_avg_event_at_decile
    FROM metrics
    WINDOW w_second_event_at_to_date AS (PARTITION BY TO_DATE(second_event_at)), w_ticket_id_by_prev2_event_at_3 AS (PARTITION BY ticket_id ORDER BY prev2_event_at_plus_2h NULLS LAST, id)
)
SELECT
    id,
    event_at,
    ticket_priority,
    prev_event_at,
    last_known_event_at,
    second_event_at_plus_2h,
    event_at_plus_2h_percent_rank,
    second_event_at,
    minutes_since_prev_event_at_plus_2h,
    first_known_event_at_plus_2h,
    second_event_at_plus_2h_3,
    event_at_decile,
    event_at_plus_2h_dense_rank,
    event_at_cume_dist,
    rows_in_second_event_at_to_date,
    event_at_cume_dist_max_2_prior,
    second_event_at_to_date_avg_event_at_cume_dist,
    next_event_at_decile,
    second_event_at_to_date_avg_event_at_decile,
    DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
FROM metrics_2
WHERE HOUR(event_at) BETWEEN 10 AND 12
ORDER BY second_event_at_plus_2h_3 DESC NULLS FIRST, ticket_priority NULLS LAST
