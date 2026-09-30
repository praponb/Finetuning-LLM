WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN location IN ('Singapore', 'Chiang Mai') THEN 'Group A' ELSE 'Other' END location_group,
        COALESCE(location, 'N/A') location_clean
    FROM departments
    WHERE location IS NOT NULL
      AND location IS DISTINCT FROM 'London'
),
filtered_departments AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN dept_name = 'Legal' THEN 1 ELSE 0 END is_legal
    FROM filtered
    WHERE dept_name IS DISTINCT FROM 'Marketing'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.level
    FROM filtered_departments s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(is_legal) OVER w_grp running_max_is_legal,
        AVG(is_legal) OVER w_grp running_avg_is_legal,
        SUM(is_legal) OVER w_grp running_sum_is_legal,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY is_legal, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_budget
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY level ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.location_group,
        s.location_clean,
        s.running_max_is_legal,
        s.running_avg_is_legal,
        s.running_sum_is_legal,
        s.running_budget,
        latest.employee_salary
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location_clean,
        running_max_is_legal,
        running_budget,
        LEAD(employee_salary, 2) OVER (PARTITION BY dept_name ORDER BY running_sum_is_legal, id) next_employee_salary,
        NTILE(10) OVER (PARTITION BY dept_name ORDER BY employee_salary DESC, id) employee_salary_bucket,
        COUNT(running_budget) OVER (PARTITION BY dept_name) dept_name_count_running_budget,
        AVG(running_sum_is_legal) OVER (ORDER BY running_avg_is_legal, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) running_sum_is_legal_avg_5_prior
    FROM latest_match_departments
    WHERE employee_salary IS NOT NULL
),
with_windows_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location_clean,
        next_employee_salary,
        dept_name_count_running_budget,
        running_sum_is_legal_avg_5_prior,
        AVG(running_sum_is_legal_avg_5_prior) OVER (PARTITION BY dept_name ORDER BY running_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_running_sum_is_legal_avg_5_prior,
        SUM(running_sum_is_legal_avg_5_prior) OVER (PARTITION BY dept_name ORDER BY running_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_running_sum_is_legal_avg_5_prior,
        MAX(running_sum_is_legal_avg_5_prior) OVER (PARTITION BY dept_name ORDER BY running_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_running_sum_is_legal_avg_5_prior,
        AVG(next_employee_salary) OVER (PARTITION BY dept_name ORDER BY running_budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) next_employee_salary_avg_6_prior
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN running_avg_running_sum_is_legal_avg_5_prior >= 360 THEN 'LARGE' ELSE 'SMALL' END AS running_avg_running_sum_is_legal_avg_5_prior_tier,
    CASE WHEN dept_name_count_running_budget >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS dept_name_count_running_budget_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(running_sum_is_legal_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS running_sum_is_legal_avg_5_prior_quartiles
FROM with_windows_departments
GROUP BY 1, 2
HAVING COUNT(*) > 1
ORDER BY dept_name_count_running_budget_tier DESC NULLS FIRST;
