WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 15
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LEAD(event_at) OVER w_actor_by_event_at AS next_event_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_dense_rank,
        NTILE(5) OVER w_event_at_year_by_event_at AS event_at_quintile,
        ROW_NUMBER() OVER w_event_at_month_by_event_at AS event_at_seq,
        RANK() OVER w_event_at_month_by_event_at_t AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS seq_no,
        ROUND(CUME_DIST() OVER w_event_at_month_by_event_at_t, 3) AS event_at_cume_dist_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_3,
        NTILE(5) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id) AS event_at_quintile_2
    FROM cleaned_events
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at NULLS LAST, id), w_event_at_year_by_event_at AS (PARTITION BY YEAR(event_at) ORDER BY event_at DESC, id), w_event_at_month_by_event_at AS (PARTITION BY MONTH(event_at) ORDER BY event_at NULLS LAST, id), w_event_at_month_by_event_at_t AS (PARTITION BY MONTH(event_at) ORDER BY event_at NULLS LAST)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    next_event_at,
    event_at_dense_rank,
    event_at_quintile,
    event_at_seq,
    event_at_rank,
    second_event_at,
    event_at_cume_dist,
    seq_no,
    event_at_cume_dist_2,
    second_event_at_2,
    second_event_at_3
FROM ranked
ORDER BY second_event_at_3 NULLS LAST
