WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.csat) AS prior_csat_12h
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.agent_id, a.opened_at
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.prior_rows_12h,
        s.prior_csat_12h,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM covered s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        prior_csat_12h,
        agent_hired_on,
        agent_team,
        LEAST(prior_csat_12h, prior_rows_12h) AS least_prior_csat_12h_prior_rows_12h,
        LENGTH(TRIM(agent_team)) AS agent_team_len,
        LOWER(TRIM(agent_team)) AS agent_team_clean
    FROM combined_details
    WHERE agent_hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.prior_csat_12h,
        s.least_prior_csat_12h_prior_rows_12h,
        s.agent_team_clean,
        su.agent_name,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on_2
    FROM labeled s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    least_prior_csat_12h_prior_rows_12h,
    agent_team_clean,
    agent_tier,
    agent_hired_on_2,
    LENGTH(TRIM(agent_team_clean)) AS agent_team_clean_len,
    LOWER(REGEXP_REPLACE(TRIM(agent_team_clean), ' ', '_')) AS agent_team_clean_slug
FROM with_details
ORDER BY least_prior_csat_12h_prior_rows_12h DESC, agent_hired_on_2 DESC
