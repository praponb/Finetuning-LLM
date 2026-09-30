WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 29
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC) AS event_at_dense_rank,
        ROW_NUMBER() OVER w_actor_by_event_at AS event_at_seq,
        RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at) AS event_at_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at) OVER w_ticket_id_by_event_at AS prev_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS next_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at, 2) OVER w_actor_by_event_at AS prev2_event_at,
        NTILE(10) OVER w_actor_ticket_id_by_event_at_2 AS event_at_decile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM valid_events
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at NULLS LAST, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at ASC, id), w_actor_ticket_id_by_event_at_2 AS (PARTITION BY actor, ticket_id ORDER BY event_at ASC, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        prev_event_at,
        first_known_event_at,
        next_event_at,
        prev2_event_at,
        second_event_at,
        minutes_since_prev_event_at,
        ROUND(event_at_seq / NULLIF(SUM(event_at_seq) OVER (PARTITION BY event_at_dense_rank, event_at_seq), 0), 4) AS event_at_seq_share_of_event_at_dense_rank_event_,
        LAG(prev_event_at) OVER (ORDER BY first_known_event_at ASC NULLS LAST, id) AS prev_prev_event_at,
        MAX(event_at_decile) OVER (PARTITION BY event_at_dense_rank, event_at_seq) AS event_at_dense_rank_event_at_seq_max_event_at_de
    FROM with_windows
)
SELECT
    id,
    event_type,
    prev_event_at,
    first_known_event_at,
    next_event_at,
    prev2_event_at,
    second_event_at,
    event_at_seq_share_of_event_at_dense_rank_event_,
    prev_prev_event_at,
    event_at_dense_rank_event_at_seq_max_event_at_de,
    YEAR(next_event_at) AS next_event_at_year,
    HOUR(next_event_at) AS next_event_at_hour
FROM sequenced
ORDER BY id, prev2_event_at
