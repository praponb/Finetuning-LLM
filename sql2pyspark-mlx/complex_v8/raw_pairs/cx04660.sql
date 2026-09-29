WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.requester_id) AS prior_requester_id_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count_2
    FROM covered s
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    prior_rows_1d,
    prior_requester_id_1d,
    ticket_count,
    ticket_count_2,
    ABS(ticket_count - 4) AS ticket_count_distance
FROM with_stats
ORDER BY prior_requester_id_1d, prior_rows_1d DESC
