WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        location,
        NTILE(10) OVER (ORDER BY budget, id) AS budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS event_seq,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share,
        SUM(budget) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM departments
)
SELECT
    id,
    dept_name,
    running_budget
FROM scored
ORDER BY id DESC NULLS LAST
