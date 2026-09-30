WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.requester_id) AS prior_requester_id_7d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM in_window
    WHERE prior_requester_id_7d > (SELECT MIN(prior_requester_id_7d) FROM in_window)
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at
    FROM filtered s
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_requester_id_7d,
        s.first_ticket_closed_at,
        su.agent_name
    FROM profiled s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    prior_requester_id_7d,
    first_ticket_closed_at,
    agent_name
FROM with_parent_agent
ORDER BY agent_id DESC, id
LIMIT 5
