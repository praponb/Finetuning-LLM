WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE event_type <> 'reopened'
      AND ticket_id <= 39
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_month,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_percent_rank,
        ROUND(event_at_month / NULLIF(SUM(event_at_month) OVER (PARTITION BY actor, ticket_id), 0), 4) AS event_at_month_share_of_actor_ticket_id,
        LAG(event_at_month, 3, 0) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS prev3_event_at_month,
        DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS event_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM recent
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at DESC)
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_month,
        s.event_at_percent_rank,
        s.event_at_month_share_of_actor_ticket_id,
        s.event_at_dense_rank,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags
    FROM sequenced s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id >= 2
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        SUM(event_at_month) OVER (PARTITION BY event_at_dense_rank ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_month,
        ROW_NUMBER() OVER (PARTITION BY event_at_dense_rank ORDER BY event_at, id) AS rn,
        SUM(event_at_percent_rank) OVER (PARTITION BY event_at_dense_rank ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_percent_rank,
        LEAD(event_at_percent_rank, 2) OVER (ORDER BY event_at_month_share_of_actor_ticket_id ASC, id) AS next2_event_at_percent_rank,
        ROUND(CUME_DIST() OVER w_event_at_dense_rank_by_event_2, 3) AS event_at_cume_dist
    FROM with_details
    WINDOW w_event_at_dense_rank_by_event_2 AS (PARTITION BY event_at_dense_rank ORDER BY event_at, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(running_event_at_month / NULLIF(event_at_percent_rank, 0), 3) AS running_event_at_month_per_event_at_percent_rank,
        ROUND(event_at_cume_dist / NULLIF(event_at_dense_rank, 0), 3) AS event_at_cume_dist_per_event_at_dense_rank
    FROM with_windows
    WHERE rn < 8
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_agent_id, event_at_dense_rank, actor, event_at_cume_dist_per_event_at_dense_rank
    FROM derived
) src
PIVOT (
    SUM(event_at_cume_dist_per_event_at_dense_rank)
    FOR actor IN ('system', 'customer')
)
