WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE actor NOT IN ('customer', 'agent')
      AND event_type = 'created'
),
with_parent_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.closed_at AS ticket_closed_at
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id), 3) AS ticket_closed_at_percent_rank,
        DENSE_RANK() OVER (ORDER BY ticket_closed_at DESC) AS ticket_closed_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id), 3) AS ticket_closed_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ticket_closed_at, id) AS ticket_closed_at_seq,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_closed_at) AS ticket_closed_at_rank,
        LEAD(event_at) OVER w_actor_by_ticket_closed_at AS next_event_at,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY actor ORDER BY ticket_closed_at, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        RANK() OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id) AS ticket_closed_at_rank_2,
        FIRST_VALUE(ticket_closed_at) OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_closed_at,
        FIRST_VALUE(ticket_closed_at) IGNORE NULLS OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ticket_closed_at,
        LAG(event_at) OVER w_event_type_event_type_or_unk AS prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY ticket_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM with_parent_ticket
    WINDOW w_actor_by_ticket_closed_at AS (PARTITION BY actor ORDER BY ticket_closed_at, id), w_event_type_event_type_or_unk AS (PARTITION BY event_type, event_type_or_unknown ORDER BY ticket_closed_at, id)
),
labeled AS (
    -- Derive helper columns
    SELECT *, INITCAP(TRIM(event_type)) AS event_type_clean
    FROM sequenced
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        actor,
        event_type_or_unknown,
        ticket_closed_at,
        ticket_closed_at_percent_rank,
        ticket_closed_at_dense_rank,
        first_known_event_at,
        ticket_closed_at_cume_dist,
        ticket_closed_at_seq,
        ticket_closed_at_rank,
        next_event_at,
        ticket_closed_at_rank_2,
        first_known_ticket_closed_at,
        prev_event_at,
        last_event_at,
        first_event_at,
        event_type_clean,
        SUM(ticket_closed_at_rank_2) OVER (PARTITION BY actor ORDER BY ticket_closed_at_cume_dist DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ticket_closed_at_rank_2_sum_4,
        MAX(ticket_closed_at_dense_rank) OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_closed_at_dense_rank_max_3_prior,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type_or_unknown, ticket_closed_at_seq)) AS distinct_actor_in_event_type_or_unknown_ticket_c,
        RANK() OVER (PARTITION BY actor ORDER BY ticket_closed_at_cume_dist DESC NULLS FIRST) AS ticket_closed_at_cume_dist_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(ticket_closed_at) ORDER BY ticket_closed_at_dense_rank, id) AS ticket_closed_at_dense_rank_dense_rank,
        ROUND(AVG(ticket_closed_at_dense_rank) OVER w_event_type_ticket_closed_at, 2) AS event_type_ticket_closed_at_rank_avg_ticket_clos,
        MIN(ticket_closed_at_rank_2) OVER (PARTITION BY YEAR(next_event_at), DATE_TRUNC('MONTH', first_ticket_closed_at) ORDER BY first_ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ticket_closed_at_rank_2_so_far
    FROM labeled
    WINDOW w_event_type_ticket_closed_at AS (PARTITION BY event_type, ticket_closed_at_rank)
)
-- Final projection
SELECT
    id,
    actor,
    event_type_or_unknown,
    ticket_closed_at,
    ticket_closed_at_percent_rank,
    ticket_closed_at_dense_rank,
    first_known_event_at,
    ticket_closed_at_cume_dist,
    ticket_closed_at_seq,
    ticket_closed_at_rank_2,
    prev_event_at,
    last_event_at,
    first_event_at,
    event_type_clean,
    ticket_closed_at_cume_dist_rank,
    ticket_closed_at_dense_rank_dense_rank,
    event_type_ticket_closed_at_rank_avg_ticket_clos,
    min_ticket_closed_at_rank_2_so_far
FROM with_trends
WHERE distinct_actor_in_event_type_or_unknown_ticket_c BETWEEN 1 AND 7
ORDER BY first_event_at NULLS LAST, ticket_closed_at_percent_rank, id
LIMIT 10
