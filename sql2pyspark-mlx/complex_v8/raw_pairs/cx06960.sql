WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 9 AND 13
      AND actor IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_event_at_month_by_event_at, 3) AS event_at_cume_dist,
        LEAD(event_at) OVER w_actor_by_event_at AS next_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LAG(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS prev2_event_at,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at) AS event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at ASC), 3) AS event_at_cume_dist_3,
        LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base_events
    WINDOW w_event_at_month_by_event_at AS (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at ASC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    last_known_event_at,
    event_at_rank,
    event_at_percent_rank,
    event_at_cume_dist_3,
    ABS(event_at_rank - 7) AS event_at_rank_distance
FROM sequenced
WHERE ticket_id <= 25
ORDER BY ticket_id DESC NULLS FIRST, event_at_rank_distance
