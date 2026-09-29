WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
without_matches AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier = 'T3'
              AND su.team NOT IN ('VIP', 'Tech')
        )
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.hired_on AS agent_hired_on
    FROM without_matches s
    JOIN support_agents su ON su.id = s.agent_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(hours_since_open) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS hours_since_open_avg_1_prior,
        MIN(hours_since_open) OVER w_priority_ticket_id AS priority_ticket_id_min_hours_since_open,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS rn,
        ROUND(hours_since_open - AVG(hours_since_open) OVER w_priority_ticket_id, 2) AS hours_since_open_vs_priority_ticket_id_avg
    FROM enriched_agent
    WINDOW w_priority_ticket_id AS (PARTITION BY priority, ticket_id)
)
SELECT
    event_type,
    agent_id,
    ticket_id,
    SUM(CASE WHEN priority = 'P1' THEN 1 ELSE 0 END) AS n_priority_p1,
    ROUND(100 * COUNT(CASE WHEN priority IN ('P3', 'P4') THEN 1 END) / COUNT(*), 1) AS pct_priority_in_p3_p4
FROM with_trends
GROUP BY GROUPING SETS ((event_type, agent_id, ticket_id), (event_type), ())
