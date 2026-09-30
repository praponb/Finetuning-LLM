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
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_1d,
        s.prior_requester_id_1d,
        su.agent_name,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM covered s
    JOIN support_agents su ON su.id = s.agent_id
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_requester_id_1d,
        s.agent_name,
        s.agent_tier,
        s.agent_team,
        su.agent_name AS agent_agent_name
    FROM with_parent_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
grouped AS (
    -- Group-level totals
    SELECT
        agent_id,
        agent_tier,
        MIN(prior_requester_id_1d) AS min_prior_requester_id_1d,
        SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'K%' THEN prior_requester_id_1d ELSE 0 END) AS prior_requester_id_1d_upper_trim_agent_name_like,
        SUM(prior_requester_id_1d) AS total_prior_requester_id_1d,
        PERCENTILE_APPROX(prior_requester_id_1d, 0.5) AS median_prior_requester_id_1d
    FROM with_parent
    GROUP BY agent_id, agent_tier
    HAVING AVG(prior_requester_id_1d) > 12
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, agent_tier, measure, value
FROM (
    SELECT agent_id, agent_tier, total_prior_requester_id_1d, prior_requester_id_1d_upper_trim_agent_name_like, min_prior_requester_id_1d
    FROM grouped
) src
UNPIVOT (
    value FOR measure IN (total_prior_requester_id_1d, prior_requester_id_1d_upper_trim_agent_name_like, min_prior_requester_id_1d)
) unp
ORDER BY agent_id, agent_tier, measure
