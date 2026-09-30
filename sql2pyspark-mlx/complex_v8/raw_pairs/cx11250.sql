WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('escalated', 'assigned')
      AND ticket_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        NTILE(5) OVER (ORDER BY event_at DESC, id) AS event_at_quintile,
        NTILE(4) OVER w_actor_by_event_at AS event_at_quartile,
        LEAD(event_at, 3) OVER w_actor_ticket_id_by_event_at AS next3_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTILE(4) OVER (ORDER BY event_at, id) AS event_at_quartile_2,
        DENSE_RANK() OVER (ORDER BY event_at DESC, id) AS event_at_dense_rank
    FROM base
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id), w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC, id), w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at, id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        ticket_id,
        actor,
        minutes_since_prev_event_at,
        second_event_at,
        second_event_at_2,
        event_at_quintile,
        event_at_quartile,
        next3_event_at,
        last_known_event_at,
        event_at_quartile_2
    FROM sequenced
    WHERE event_at_dense_rank <= 2
)
SELECT
    id,
    ticket_id,
    actor,
    minutes_since_prev_event_at,
    second_event_at,
    second_event_at_2,
    event_at_quintile,
    event_at_quartile,
    next3_event_at,
    event_at_quartile_2,
    CASE WHEN event_at_quartile >= 6 THEN 'HIGH' WHEN event_at_quartile >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS event_at_quartile_tier,
    ROUND((UNIX_TIMESTAMP(second_event_at_2) - UNIX_TIMESTAMP(next3_event_at)) / 3600, 1) AS hours_next3_event_at_to_second_event_at_2
FROM top_rows
ORDER BY actor DESC NULLS FIRST, id DESC
LIMIT 5
