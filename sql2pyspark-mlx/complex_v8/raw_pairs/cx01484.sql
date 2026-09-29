WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE event_type IN ('escalated', 'replied')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(event_at) OVER w_ticket_id_actor_by_event_at AS next_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at NULLS LAST, id) AS event_at_seq,
        NTILE(10) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id) AS event_at_decile,
        NTILE(3) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id) AS event_at_tercile,
        RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST) AS event_at_rank,
        DENSE_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER (PARTITION BY actor ORDER BY event_at NULLS LAST) AS event_at_rank_2
    FROM scoped
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        next_event_at,
        event_at_seq,
        event_at_tercile,
        event_at_rank,
        first_event_at,
        event_at_rank_2,
        LEAD(event_at_dense_rank) OVER (ORDER BY event_at_rank DESC, id) AS next_event_at_dense_rank,
        event_at_rank_2 - LAG(event_at_rank_2) OVER (PARTITION BY event_at_rank ORDER BY next_event_at DESC, id) AS event_at_rank_2_change,
        NTH_VALUE(event_at_dense_rank, 2) OVER (PARTITION BY event_at_seq, event_at_tercile ORDER BY event_at_dense_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_dense_rank,
        LAG(event_at_decile) OVER (PARTITION BY event_at_seq, event_at_tercile ORDER BY event_at_dense_rank DESC NULLS FIRST, id) AS prev_event_at_decile,
        SUM(event_at_seq) OVER (PARTITION BY event_at_rank ORDER BY next_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_seq
    FROM sequenced
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    next_event_at,
    event_at_tercile,
    event_at_rank,
    event_at_rank_2,
    next_event_at_dense_rank,
    event_at_rank_2_change,
    CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
    SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
FROM metrics
ORDER BY event_at_tercile NULLS LAST
