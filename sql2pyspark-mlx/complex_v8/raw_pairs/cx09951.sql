WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags,
        TO_DATE(closed_at) AS closed_at_date,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE priority IN ('P3', 'P1')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(requester_id) OVER (ORDER BY csat_minus_requester_id ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_sum_3_prior,
        MAX(csat_minus_requester_id) OVER (PARTITION BY agent_id, channel) AS agent_id_channel_max_csat_minus_requester_id,
        SUM(csat_minus_requester_id) OVER (PARTITION BY channel ORDER BY csat_minus_requester_id ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_minus_requester_id_sum_3_prior
    FROM scoped
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.csat_minus_requester_id,
        s.agent_id_channel_max_csat_minus_requester_id,
        s.csat_minus_requester_id_sum_3_prior,
        su.team AS agent_team
    FROM sequenced s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'Onboarding'
),
with_activity AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IS NOT NULL
              AND UPPER(TRIM(su.agent_name)) LIKE 'K%'
        )
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS min_ticket_requester_id
    FROM with_activity s
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        s.csat_minus_requester_id,
        s.agent_id_channel_max_csat_minus_requester_id,
        s.csat_minus_requester_id_sum_3_prior,
        s.agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM summarized_rows s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on IS NOT NULL
    WHERE su.hired_on IS NOT NULL
)
SELECT
    agent_tier,
    agent_id,
    agent_team,
    ROUND(SUM(agent_id_channel_max_csat_minus_requester_id) / NULLIF(SUM(csat_minus_requester_id_sum_3_prior), 0), 3) AS agent_id_channel_max_csat_minus_requester_id_to_
FROM combined_details_agent
GROUP BY GROUPING SETS ((agent_tier, agent_id, agent_team), (agent_tier), ())
ORDER BY agent_tier NULLS LAST, agent_id NULLS LAST, agent_team NULLS LAST
