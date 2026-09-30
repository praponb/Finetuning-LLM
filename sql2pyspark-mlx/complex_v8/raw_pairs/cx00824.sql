WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        actor_event_type_label,
        LEAD(event_at, 3) OVER w_event_type_ticket_id_by_even AS next3_event_at,
        FIRST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY actor, ticket_id ORDER BY event_at_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_week,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at, id), 3) AS event_at_cume_dist,
        LAST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at_week) OVER (ORDER BY event_at, id) AS next_event_at_week,
        LAG(event_at_week, 3) OVER w_all_by_event_at AS prev3_event_at_week,
        ROW_NUMBER() OVER w_event_type_ticket_id_by_even AS rn,
        NTILE(10) OVER (PARTITION BY actor, ticket_id ORDER BY event_at_week, id) AS event_at_week_decile,
        DENSE_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_week DESC) AS event_at_week_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER w_event_type_ticket_id_by_even)) / 60, 1) AS minutes_since_prev_event_at_week,
        LAG(event_at_week) OVER (ORDER BY event_at, id) AS prev_event_at_week
    FROM base
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at_week DESC, id), w_all_by_event_at_ties AS (ORDER BY event_at), w_all_by_event_at AS (ORDER BY event_at, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at_rank DESC, id) AS event_at_rank_tercile,
        MIN(event_at_week_decile) OVER w_event_at_week_dense_rank AS event_at_week_dense_rank_min_event_at_week_decil,
        ROUND(CUME_DIST() OVER (PARTITION BY rn ORDER BY first_known_event_at, id), 3) AS first_known_event_at_cume_dist
    FROM metrics
    WINDOW w_event_at_week_dense_rank AS (PARTITION BY event_at_week_dense_rank), w_rn_by_first_known_event_at AS (PARTITION BY rn ORDER BY first_known_event_at, id)
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    actor_event_type_label,
    first_known_event_at_week,
    event_at_rank,
    event_at_cume_dist,
    last_event_at,
    next_event_at_week,
    prev3_event_at_week,
    rn,
    event_at_week_decile,
    event_at_week_dense_rank,
    minutes_since_prev_event_at_week,
    prev_event_at_week,
    event_at_rank_tercile,
    event_at_week_dense_rank_min_event_at_week_decil,
    first_known_event_at_cume_dist
FROM windowed
ORDER BY event_at_week_dense_rank_min_event_at_week_decil
