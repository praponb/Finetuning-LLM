WITH metrics AS (
    SELECT
        *,
        AVG(budget) OVER w_recent AS running_avg_budget,
        COUNT(budget) OVER w_recent AS running_count_budget,
        MIN(budget) OVER w_recent AS running_min_budget
    FROM departments
    WHERE dept_name IN ('Finance', 'Engineering', 'Marketing')
    WINDOW w_recent AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    dept_name,
    location,
    budget,
    running_avg_budget,
    running_min_budget
FROM metrics;
