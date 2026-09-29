WITH prepared_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team IS NOT NULL
      AND tier IN ('T3', 'T1')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        tier,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on NULLS LAST, id), 3) AS hired_on_percent_rank,
        DENSE_RANK() OVER (ORDER BY hired_on NULLS LAST) AS hired_on_dense_rank,
        ROW_NUMBER() OVER (ORDER BY hired_on NULLS LAST, id) AS rn,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on NULLS LAST, id)) AS days_since_prev_hired_on
    FROM prepared_agents
),
unique_rows AS (
    -- Keep the earliest row per team
    SELECT
        id,
        team,
        tier,
        hired_on_percent_rank,
        hired_on_dense_rank,
        rn,
        days_since_prev_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY rn, id) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM unique_rows
    WHERE hired_on_dense_rank = 1
)
-- Final projection
SELECT
    id,
    team,
    tier,
    hired_on_percent_rank,
    hired_on_dense_rank,
    rn,
    days_since_prev_hired_on,
    days_since_prev_hired_on - hired_on_dense_rank AS days_since_prev_hired_on_minus_hired_on_dense_ra
FROM top_per_group
ORDER BY team DESC, hired_on_dense_rank, id
LIMIT 10
