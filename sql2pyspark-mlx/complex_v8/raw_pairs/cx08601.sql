WITH recent AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref
    FROM support_agents
    WHERE tier = 'T1'
      AND team NOT IN ('Onboarding', 'Billing')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        team_ref,
        UPPER(TRIM(team)) AS team_clean
    FROM recent
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        hired_on,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on DESC, id) AS seq_no,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY team ORDER BY hired_on DESC, id)) AS days_since_prev_hired_on,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        NTILE(5) OVER (PARTITION BY team ORDER BY hired_on DESC, id) AS hired_on_quintile
    FROM enriched
)
SELECT
    team,
    hired_on_quintile,
    seq_no,
    SUM(seq_no) AS total_seq_no,
    MIN(second_hired_on) AS first_second_hired_on
FROM windowed
GROUP BY ROLLUP(team, hired_on_quintile, seq_no)
