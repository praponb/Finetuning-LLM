WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        COUNT(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget,
        MIN(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget,
        STDDEV_SAMP(budget) OVER (PARTITION BY dept_name) AS dept_name_stddev_samp_budget
    FROM departments
)
SELECT
    id,
    location,
    running_count_budget,
    running_avg_budget,
    dept_name_stddev_samp_budget
FROM enriched
ORDER BY running_avg_budget NULLS FIRST;
