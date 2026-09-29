WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat BETWEEN 2 AND 4
),
enriched AS (
    -- Attach agent attributes
    SELECT s.*, su.team AS agent_team, su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        agent_team,
        agent_hired_on,
        LAG(agent_hired_on) OVER (PARTITION BY YEAR(closed_at) ORDER BY csat DESC, id) AS prev_agent_hired_on,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LAG(closed_at, 2) OVER w_agent_hired_on_year_opened_a_2 AS prev2_closed_at,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY channel)) AS distinct_agent_team_in_channel,
        SUM(csat) OVER (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY csat DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_sum_3,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY csat DESC, id)) AS days_since_prev_agent_hired_on,
        RANK() OVER w_agent_hired_on_year_opened_a_4 AS csat_rank,
        LAG(csat, 3, 0) OVER (PARTITION BY csat, agent_team ORDER BY opened_at DESC, id) AS prev3_csat,
        DENSE_RANK() OVER (PARTITION BY csat, agent_team ORDER BY opened_at DESC) AS opened_at_dense_rank,
        MAX(csat) OVER (PARTITION BY YEAR(closed_at) ORDER BY csat DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_max_3_prior,
        RANK() OVER (PARTITION BY csat, agent_team ORDER BY opened_at DESC) AS opened_at_rank,
        ROUND(csat - AVG(csat) OVER (PARTITION BY channel), 2) AS csat_vs_channel_avg,
        MAX(csat) OVER (PARTITION BY YEAR(closed_at) ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far
    FROM enriched
    WINDOW w_agent_hired_on_year_opened_a_2 AS (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY csat DESC, id), w_agent_hired_on_year_opened_a_4 AS (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY csat DESC)
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    agent_team,
    agent_hired_on,
    prev_agent_hired_on,
    second_closed_at,
    prev2_closed_at,
    distinct_agent_team_in_channel,
    csat_sum_3,
    csat_rank,
    prev3_csat,
    opened_at_dense_rank,
    csat_max_3_prior,
    opened_at_rank,
    csat_vs_channel_avg,
    max_csat_so_far
FROM sequenced
ORDER BY csat_rank DESC
