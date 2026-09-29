WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 1 DAYS AS event_at_plus_1d,
        CASE WHEN event_type IN ('replied', 'assigned') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_type = 'replied'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_group,
        DENSE_RANK() OVER (PARTITION BY event_type, actor_group ORDER BY event_at_plus_1d DESC) AS event_at_plus_1d_dense_rank,
        RANK() OVER (PARTITION BY event_type, actor_group ORDER BY event_at_plus_1d DESC, id) AS event_at_plus_1d_rank,
        ROUND((UNIX_TIMESTAMP(event_at_plus_1d) - UNIX_TIMESTAMP(LAG(event_at_plus_1d) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at_plus_1d) ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_plus_1d,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, actor_group ORDER BY event_at_plus_1d DESC, id), 3) AS event_at_plus_1d_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at_plus_1d) ORDER BY event_at), 3) AS event_at_cume_dist
    FROM valid
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_group,
        event_at_plus_1d_rank,
        minutes_since_prev_event_at_plus_1d,
        event_at_plus_1d_percent_rank,
        event_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY event_at_plus_1d_dense_rank, event_type ORDER BY event_at_plus_1d_dense_rank DESC NULLS FIRST), 3) AS event_at_plus_1d_dense_rank_cume_dist,
        COUNT(*) OVER w_event_type_actor_group AS rows_in_event_type_actor_group,
        SUM(event_at_cume_dist) OVER (PARTITION BY actor, event_at_plus_1d_dense_rank ORDER BY event_at_plus_1d_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_cume_dist_sum_6_prior,
        LEAD(event_at) OVER w_actor_event_at_plus_1d_dense_2 AS next_event_at,
        LAG(event_at_plus_1d_dense_rank, 2) OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at DESC, id) AS prev2_event_at_plus_1d_dense_rank,
        RANK() OVER (PARTITION BY event_at_plus_1d_dense_rank, event_type ORDER BY event_at_plus_1d_dense_rank DESC NULLS FIRST) AS event_at_plus_1d_dense_rank_rank
    FROM ranked
    WINDOW w_event_type_actor_group AS (PARTITION BY event_type, actor_group), w_actor_event_at_plus_1d_dense_2 AS (PARTITION BY actor, event_at_plus_1d_dense_rank ORDER BY event_at_plus_1d_rank, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_windows
    WHERE event_at_plus_1d_rank <= 5
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.actor_group,
        s.minutes_since_prev_event_at_plus_1d,
        s.event_at_cume_dist,
        s.event_at_plus_1d_dense_rank_cume_dist,
        s.event_at_cume_dist_sum_6_prior,
        s.next_event_at,
        s.prev2_event_at_plus_1d_dense_rank,
        s.event_at_plus_1d_dense_rank_rank,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type,
        t.actor AS event_actor
    FROM first_rows s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        actor,
        actor_group,
        event_at_cume_dist,
        event_at_plus_1d_dense_rank_cume_dist,
        prev2_event_at_plus_1d_dense_rank,
        event_id,
        event_event_at,
        DATEDIFF(DATE '2025-12-31', next_event_at) AS days_since_next_event_at
    FROM line_level
)
SELECT
    id,
    actor,
    actor_group,
    event_at_cume_dist,
    event_at_plus_1d_dense_rank_cume_dist,
    event_event_at,
    days_since_next_event_at
FROM annotated
WHERE actor_group = 'group_b'
ORDER BY event_at_cume_dist NULLS LAST
