SELECT
    *,
    AVG(budget) OVER w_grp AS running_avg_budget,
    MIN(budget) OVER w_grp AS running_min_budget,
    COUNT(budget) OVER w_grp AS running_count_budget
FROM departments
WINDOW w_grp AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
