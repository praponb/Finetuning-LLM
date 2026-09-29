WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.requester_id) AS prior_requester_id_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_requester_id_3d,
        su.agent_name
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.agent_name IS NOT NULL
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    agent_name
FROM with_details
ORDER BY agent_name, agent_id DESC
