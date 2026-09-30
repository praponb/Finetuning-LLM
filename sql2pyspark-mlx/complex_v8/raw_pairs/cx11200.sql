WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        event_type_prefix,
        LAG(event_at) OVER w_actor_event_type_by_event_at AS prev_event_at,
        NTILE(10) OVER w_actor_event_type_by_event_at AS event_at_decile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2,
        LEAD(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM recent_events
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at, id), w_all_by_event_at AS (ORDER BY event_at, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        event_type_ref,
        event_at_decile,
        first_known_event_at,
        first_known_event_at_2,
        next_event_at,
        minutes_since_prev_event_at,
        RANK() OVER (ORDER BY last_event_at) AS last_event_at_rank,
        ROUND(AVG(minutes_since_prev_event_at) OVER (ORDER BY last_event_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS minutes_since_prev_event_at_avg_4,
        ROUND(event_at_decile - AVG(event_at_decile) OVER w_actor_ticket_id, 2) AS event_at_decile_vs_actor_ticket_id_avg
    FROM with_windows
    WINDOW w_actor_ticket_id AS (PARTITION BY actor, ticket_id)
)
SELECT
    id,
    event_type,
    event_type_ref,
    first_known_event_at,
    first_known_event_at_2,
    next_event_at,
    minutes_since_prev_event_at,
    last_event_at_rank,
    event_at_decile_vs_actor_ticket_id_avg,
    CASE WHEN last_event_at_rank >= 7 THEN 'large' WHEN last_event_at_rank >= 4 THEN 'medium' ELSE 'small' END AS last_event_at_rank_tier
FROM sequenced
