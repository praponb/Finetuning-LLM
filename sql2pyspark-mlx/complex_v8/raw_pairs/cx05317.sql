WITH prepared AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CONCAT(agent_name, '#', CAST(id AS STRING)) AS agent_name_ref
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 4
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(hired_on, 2) OVER w_all_by_hired_on AS next2_hired_on,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY hired_on DESC) AS hired_on_dense_rank,
        LAG(hired_on) OVER (ORDER BY hired_on NULLS LAST, id) AS prev_hired_on,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on NULLS LAST), 3) AS hired_on_percent_rank,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on
    FROM prepared
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on NULLS LAST, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT prev_hired_on, next2_hired_on, team, hired_on_dense_rank
    FROM sequenced
) src
PIVOT (
    SUM(hired_on_dense_rank)
    FOR team IN ('Billing', 'Tech', 'Onboarding', 'VIP')
)
ORDER BY prev_hired_on, next2_hired_on
