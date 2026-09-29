WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 17
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_actor_ticket_id_by_event_at, 3) AS event_at_cume_dist,
        NTILE(10) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_decile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at, id) AS seq_no,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS rn,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAG(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev3_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at), 3) AS event_at_cume_dist_2
    FROM valid_events
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at DESC)
)
SELECT
    id,
    ticket_id,
    event_at_cume_dist,
    event_at_decile,
    event_at_rank,
    event_at_percent_rank,
    seq_no,
    minutes_since_prev_event_at,
    second_event_at_2,
    first_event_at,
    first_known_event_at,
    event_at_cume_dist_2,
    TO_DATE(first_known_event_at) AS first_known_event_at_date,
    ROUND(event_at_cume_dist_2 * 1.5, 2) AS event_at_cume_dist_2_adj
FROM sequenced
ORDER BY id
LIMIT 5
