WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor <> 'agent'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id
    FROM scoped_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAG(event_at, 3) OVER (ORDER BY event_at DESC, id) AS prev3_event_at,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank
    FROM combined_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id), w_all_by_event_at_ties AS (ORDER BY event_at DESC)
),
combined_details_2 AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.ticket_agent_id,
        s.event_at_cume_dist,
        s.prev_event_at,
        s.last_event_at,
        s.prev3_event_at,
        s.event_at_dense_rank,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM ranked s
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM combined_details_2 s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
              AND t.priority IS NOT NULL
        )
      AND s.agent_team NOT IN ('Onboarding', 'VIP')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(event_at_dense_rank) OVER (PARTITION BY agent_team, event_type ORDER BY prev3_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_dense_rank,
        ROUND(event_at_cume_dist / NULLIF(SUM(event_at_cume_dist) OVER w_ticket_agent_id_ticket_id, 0), 4) AS event_at_cume_dist_share_of_ticket_agent_id_tick
    FROM without_matches
    WINDOW w_ticket_agent_id_ticket_id AS (PARTITION BY ticket_agent_id, ticket_id)
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    COALESCE(actor_or_unknown, 'ALL') AS actor_or_unknown,
    GROUPING(event_type) AS is_total_event_type,
    MAX(event_at_dense_rank) AS max_event_at_dense_rank,
    COUNT(*) FILTER (WHERE UPPER(TRIM(agent_name)) LIKE 'G%') AS n_upper_trim_agent_name_like_g,
    COUNT(*) AS n_rows
FROM with_trends
GROUP BY CUBE(event_type, actor_or_unknown)
