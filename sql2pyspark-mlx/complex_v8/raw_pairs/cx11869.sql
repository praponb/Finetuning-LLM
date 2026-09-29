WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.priority IN ('P3', 'P2', 'P1')) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_csat,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM joined_ranges s
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_3d,
    ticket_priority_count,
    max_ticket_csat,
    ticket_count,
    ABS(prior_csat_3d - 11) AS prior_csat_3d_distance
FROM profiled
ORDER BY opened_at DESC NULLS FIRST, agent_id DESC
