WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ROUND(PERCENT_RANK() OVER w_actor_by_event_at, 3) AS event_at_percent_rank,
        NTILE(3) OVER w_ticket_id_event_type_by_even AS event_at_tercile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id) AS prev2_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS event_at_decile,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS rn,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id) AS prev_event_at
    FROM prepared_events
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id), w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at ASC, id), w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at DESC)
),
with_windows_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_tercile,
        minutes_since_prev_event_at,
        first_event_at,
        second_event_at,
        seq_no,
        event_at_decile,
        first_event_at_2,
        minutes_since_prev_event_at_2,
        rn,
        prev_event_at,
        ROUND(event_at_percent_rank / NULLIF(SUM(event_at_percent_rank) OVER (PARTITION BY event_type, event_at_tercile), 0), 4) AS event_at_percent_rank_share_of_event_type_event_,
        ROUND(CUME_DIST() OVER (ORDER BY first_event_at DESC, id), 3) AS first_event_at_cume_dist,
        ROUND(event_at_percent_rank - AVG(event_at_percent_rank) OVER (PARTITION BY event_type, event_at_tercile), 2) AS event_at_percent_rank_vs_event_type_event_at_ter,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type, event_at_tercile)) AS distinct_actor_in_event_type_event_at_tercile,
        NTH_VALUE(event_at_percent_rank, 2) OVER (ORDER BY event_at_percent_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_percent_rank
    FROM with_windows
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    minutes_since_prev_event_at,
    second_event_at,
    seq_no,
    event_at_decile,
    first_event_at_2,
    minutes_since_prev_event_at_2,
    rn,
    prev_event_at,
    event_at_percent_rank_share_of_event_type_event_,
    event_at_percent_rank_vs_event_type_event_at_ter,
    distinct_actor_in_event_type_event_at_tercile
FROM with_windows_2
ORDER BY event_type DESC
