WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type_ref,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST) AS event_at_rank,
        NTILE(4) OVER w_ticket_id_event_type_by_even_3 AS event_at_quartile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at, 3) OVER w_ticket_id_event_type_by_even_4 AS prev3_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST) AS event_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2
    FROM base_events
    WINDOW w_ticket_id_event_type_by_even_3 AS (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id), w_ticket_id_event_type_by_even_4 AS (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    second_event_at,
    minutes_since_prev_event_at,
    last_event_at,
    event_at_rank,
    event_at_quartile,
    last_known_event_at,
    event_at_dense_rank,
    minutes_since_prev_event_at_2,
    second_event_at_2,
    last_known_event_at_2,
    COALESCE(event_at_quartile, 0) AS event_at_quartile_filled,
    ABS(event_at_rank - 5) AS event_at_rank_distance
FROM metrics
ORDER BY ticket_id
