WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'reopened'
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        su.tier AS agent_tier
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(event_at, 3) OVER w_agent_tier_by_event_at AS prev3_event_at,
        DENSE_RANK() OVER (PARTITION BY agent_tier ORDER BY event_at DESC) AS event_at_dense_rank
    FROM with_details_ticket
    WINDOW w_agent_tier_by_event_at AS (PARTITION BY agent_tier ORDER BY event_at DESC, id)
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'reopened'
              AND HOUR(t.event_at) BETWEEN 6 AND 10
        )
)
SELECT
    COALESCE(actor, 'ALL') AS actor,
    event_at_dense_rank,
    MIN(prev3_event_at) AS first_prev3_event_at,
    SUM(CASE WHEN actor IN ('system', 'customer') THEN event_at_dense_rank ELSE 0 END) AS event_at_dense_rank_actor_in_system_customer
FROM without_matches
GROUP BY GROUPING SETS ((actor, event_at_dense_rank), (actor), (event_at_dense_rank))
