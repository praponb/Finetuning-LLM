WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        location,
        SUM(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) budget_share,
        NTILE(5) OVER (PARTITION BY location, dept_name ORDER BY budget, id) budget_bucket
    FROM departments
    WHERE dept_name IN ('Finance', 'Marketing')
)
SELECT
    id,
    location,
    budget_share
FROM with_windows
ORDER BY budget_share DESC NULLS LAST
