WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.requester_id) AS prior_requester_id_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.prior_requester_id_12h,
        su.agent_name
    FROM in_window s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT id, agent_id, LOWER(TRIM(agent_name)) AS agent_name_clean
FROM with_details
