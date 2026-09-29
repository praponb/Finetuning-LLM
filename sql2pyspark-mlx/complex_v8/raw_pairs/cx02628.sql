WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC) AS event_at_dense_rank,
        DENSE_RANK() OVER (ORDER BY event_at DESC, id) AS event_at_dense_rank_2,
        NTILE(3) OVER w_ticket_id_actor_by_event_at AS event_at_tercile,
        ROW_NUMBER() OVER w_event_at_month_event_type_by AS seq_no,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at ASC) AS event_at_dense_rank_3,
        ROUND(PERCENT_RANK() OVER w_actor_event_type_by_event_at_2, 3) AS event_at_percent_rank,
        LEAD(event_at, 2) OVER w_actor_event_type_by_event_at_3 AS next2_event_at
    FROM base
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), w_event_at_month_event_type_by AS (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at ASC, id), w_actor_event_type_by_event_at_2 AS (PARTITION BY actor, event_type ORDER BY event_at DESC), w_actor_event_type_by_event_at_3 AS (PARTITION BY actor, event_type ORDER BY event_at DESC, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(last_event_at) ORDER BY event_at_dense_rank_3), 3) AS event_at_dense_rank_3_cume_dist,
        RANK() OVER w_seq_no_event_at_rank_by_firs AS first_event_at_rank,
        MAX(event_at_rank) OVER (PARTITION BY event_type) AS event_type_max_event_at_rank,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type
    FROM ranked
    WINDOW w_seq_no_event_at_rank_by_firs AS (PARTITION BY seq_no, event_at_rank ORDER BY first_event_at, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    last_event_at,
    event_at_dense_rank,
    event_at_dense_rank_2,
    seq_no,
    event_at_rank,
    first_event_at,
    event_at_dense_rank_3,
    next2_event_at,
    first_event_at_rank,
    event_type_max_event_at_rank,
    rows_in_event_type
FROM metrics
ORDER BY event_type_max_event_at_rank, id
LIMIT 10
