WITH recent AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type = 'escalated'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        LAG(event_at, 2) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS prev2_event_at,
        LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC NULLS FIRST, id) AS prev_event_at,
        LEAD(event_at, 3) OVER (ORDER BY event_at DESC, id) AS next3_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND(PERCENT_RANK() OVER w_event_type_actor_by_event_at_2, 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, actor ORDER BY event_at, id), 3) AS event_at_cume_dist_2
    FROM recent
    WINDOW w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    prev2_event_at,
    prev_event_at,
    next3_event_at,
    second_event_at,
    second_event_at_2,
    event_at_percent_rank,
    event_at_cume_dist_2,
    event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM sequenced
ORDER BY prev2_event_at DESC NULLS FIRST, second_event_at DESC
