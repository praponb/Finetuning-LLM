WITH in_window AS (
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
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.prior_rows_3d,
        su.agent_name,
        su.tier AS agent_tier
    FROM qualified s
    JOIN support_agents su ON su.id = s.agent_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        prior_rows_3d,
        agent_tier,
        CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref,
        CASE WHEN agent_tier IN ('T3', 'T1') THEN 'group_a' WHEN agent_tier = 'T2' THEN 'group_b' ELSE 'other' END AS agent_tier_group
    FROM combined_details_agent
)
-- Final projection
SELECT
    id,
    agent_id,
    prior_rows_3d,
    agent_tier,
    agent_tier_group,
    LOWER(REGEXP_REPLACE(TRIM(agent_tier_group), ' ', '_')) AS agent_tier_group_slug
FROM scored
ORDER BY prior_rows_3d DESC, agent_tier_group_slug DESC, id
LIMIT 20
