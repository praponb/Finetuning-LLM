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
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_7d,
        su.tier AS agent_tier,
        su.agent_name
    FROM in_window s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IS NOT NULL
)
SELECT
    id,
    opened_at,
    agent_tier,
    agent_name,
    DATE_TRUNC('WEEK', opened_at) AS opened_at_week,
    TO_DATE(opened_at) AS opened_at_date
FROM joined_agent
ORDER BY agent_name, id NULLS LAST
