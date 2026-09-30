WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 08:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS event_at_dense_rank,
        LAG(event_at, 2) OVER w_event_type_ticket_id_by_even AS prev2_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY YEAR(event_at) ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(10) OVER w_event_type_ticket_id_by_even AS event_at_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER (PARTITION BY YEAR(event_at) ORDER BY event_at ASC NULLS LAST, id) AS prev_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY YEAR(event_at) ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY event_at ASC NULLS LAST), 3) AS event_at_percent_rank,
        NTILE(10) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS event_at_decile_2
    FROM scoped_events
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_dense_rank,
        prev2_event_at,
        second_event_at,
        minutes_since_prev_event_at,
        prev_event_at,
        first_known_event_at,
        event_at_decile_2,
        RANK() OVER (PARTITION BY event_at_decile_2, YEAR(event_at) ORDER BY first_known_event_at ASC) AS first_known_event_at_rank,
        DENSE_RANK() OVER (PARTITION BY event_at_decile_2, YEAR(event_at) ORDER BY first_known_event_at ASC) AS first_known_event_at_dense_rank,
        ROW_NUMBER() OVER w_event_at_decile_2_event_at_y_2 AS first_known_event_at_seq,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at_decile_2 DESC NULLS FIRST, id) AS seq_no_2,
        ROUND(AVG(event_at_decile) OVER w_ticket_id, 2) AS ticket_id_avg_event_at_decile,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id
    FROM ranked
    WINDOW w_event_at_decile_2_event_at_y_2 AS (PARTITION BY event_at_decile_2, YEAR(event_at) ORDER BY first_known_event_at ASC, id), w_ticket_id AS (PARTITION BY ticket_id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_at_dense_rank,
    second_event_at,
    prev_event_at,
    first_known_event_at,
    event_at_decile_2,
    first_known_event_at_rank,
    first_known_event_at_dense_rank,
    first_known_event_at_seq,
    seq_no_2,
    ticket_id_avg_event_at_decile,
    distinct_actor_in_ticket_id
FROM windowed
ORDER BY event_at
