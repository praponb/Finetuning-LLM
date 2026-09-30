WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor = 'customer' OR event_at < TIMESTAMP '2025-02-24 12:00:00')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS seq_no
    FROM filtered
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC, id)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MIN(event_at) AS first_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at_2
    FROM sequenced s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_at_rank,
        s.event_at_cume_dist,
        s.event_at_rank_2,
        s.first_event_at,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.agent_name,
        su.team AS agent_team
    FROM summarized_rows s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    event_at_rank,
    ticket_id,
    COALESCE(agent_team, 'ALL') AS agent_team,
    GROUPING_ID() AS grouping_level,
    SUM(event_at_rank) AS total_event_at_rank
FROM combined_details
GROUP BY GROUPING SETS ((event_at_rank, ticket_id, agent_team), (event_at_rank), (ticket_id))
ORDER BY event_at_rank NULLS LAST, ticket_id NULLS LAST, agent_team NULLS LAST
