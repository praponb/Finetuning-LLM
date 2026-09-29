WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE actor IS NOT NULL
),
with_details_ticket AS (
    -- Join tickets
    SELECT s.*, t.tags AS ticket_tags
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_ref,
        ticket_tags,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        DENSE_RANK() OVER w_ticket_id_by_event_at_ties AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at ASC, id), 3) AS event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at) OVER (ORDER BY event_at ASC, id) AS prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        DENSE_RANK() OVER (ORDER BY event_at ASC) AS event_at_dense_rank_2,
        LEAD(event_at) OVER (ORDER BY event_at ASC, id) AS next_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM with_details_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id), w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM sequenced
    WHERE next_event_at < TIMESTAMP '2025-02-12 11:00:00'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        actor,
        actor_ref,
        ticket_tags,
        event_at_dense_rank,
        second_event_at,
        second_event_at_2,
        event_at_percent_rank,
        event_at_percent_rank_2,
        first_event_at,
        first_event_at_2,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        SUM(event_at_cume_dist) OVER (PARTITION BY ticket_id ORDER BY last_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_cume_dist,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at) ORDER BY event_at_percent_rank DESC, id) AS event_at_percent_rank_rank,
        ROUND(event_at_percent_rank - AVG(event_at_percent_rank) OVER (PARTITION BY actor), 2) AS event_at_percent_rank_vs_actor_avg,
        NTH_VALUE(event_at_percent_rank, 2) OVER (PARTITION BY ticket_id ORDER BY last_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(next_event_at) ORDER BY first_event_at_2 NULLS LAST), 3) AS first_event_at_2_percent_rank
    FROM tagged
    WINDOW w_actor AS (PARTITION BY actor)
)
SELECT
    id,
    actor,
    ticket_tags,
    event_at_dense_rank,
    second_event_at,
    second_event_at_2,
    first_event_at,
    first_event_at_2,
    running_event_at_cume_dist,
    event_at_percent_rank_rank,
    event_at_percent_rank_vs_actor_avg,
    first_event_at_2_percent_rank,
    ARRAY_CONTAINS(ticket_tags, 'login') AS has_login
FROM with_windows
WHERE rows_in_actor <= 18
ORDER BY first_event_at NULLS LAST
