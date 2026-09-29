WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('escalated', 'replied') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type IN ('reopened', 'assigned')
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, t.priority AS ticket_priority
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at) OVER w_event_type_group_ticket_prio AS prev_event_at,
        RANK() OVER (PARTITION BY event_type_group, ticket_priority ORDER BY event_at) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_priority ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        RANK() OVER (PARTITION BY ticket_priority ORDER BY event_at) AS event_at_rank_2,
        RANK() OVER w_event_type_group_ticket_prio AS event_at_rank_3,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type_group, ticket_priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(5) OVER (ORDER BY event_at, id) AS event_at_quintile,
        NTILE(10) OVER (PARTITION BY event_type_group, ticket_priority ORDER BY event_at, id) AS event_at_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_group_ticket_prio)) / 60, 1) AS minutes_since_prev_event_at_3,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank_4
    FROM enriched
    WINDOW w_event_type_group_ticket_prio AS (PARTITION BY event_type_group, ticket_priority ORDER BY event_at, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY ticket_priority ORDER BY second_event_at DESC, id) AS second_event_at_quintile,
        LEAD(event_at_rank_2) OVER (PARTITION BY event_at_decile ORDER BY minutes_since_prev_event_at, id) AS next_event_at_rank_2,
        SUM(event_at_rank_4) OVER (PARTITION BY event_at_decile ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_at_rank_4_sum_7,
        SUM(event_at_quintile) OVER (PARTITION BY event_at_quintile) AS event_at_quintile_sum_event_at_quintile,
        ROUND(event_at_rank / NULLIF(SUM(event_at_rank) OVER (PARTITION BY actor), 0), 4) AS event_at_rank_share_of_actor,
        DENSE_RANK() OVER (ORDER BY minutes_since_prev_event_at DESC) AS minutes_since_prev_event_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_at_rank_2 ORDER BY event_at_rank_3, id), 3) AS event_at_rank_3_cume_dist
    FROM windowed
)
SELECT
    id,
    ticket_id,
    event_type,
    actor_ref,
    ticket_priority,
    second_event_at,
    minutes_since_prev_event_at,
    first_event_at,
    prev_event_at,
    event_at_rank,
    event_at_rank_3,
    first_known_event_at,
    event_at_quintile,
    minutes_since_prev_event_at_3,
    event_at_rank_4,
    second_event_at_quintile,
    next_event_at_rank_2,
    event_at_quintile_sum_event_at_quintile,
    event_at_rank_share_of_actor,
    minutes_since_prev_event_at_dense_rank,
    event_at_rank_3_cume_dist
FROM with_windows
ORDER BY event_at_rank_3 DESC, id
LIMIT 20
