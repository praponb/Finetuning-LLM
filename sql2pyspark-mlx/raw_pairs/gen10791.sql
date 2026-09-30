WITH enriched AS (
    SELECT
        s.id,
        s.full_name,
        s.dept_id,
        s.job_title,
        s.hire_date,
        s.is_remote,
        s.level,
        d.location,
        d.dept_name,
        d.budget
    FROM employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
    WHERE d.location = 'Bangkok'
),
enriched_employees AS (
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date), dept_name ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_avg_3,
        LAG(budget, 2) OVER w_recent AS prev_budget
    FROM enriched
    WINDOW w_recent AS (PARTITION BY YEAR(hire_date) ORDER BY budget, id)
),
joined AS (
    SELECT
        a.*,
        b.budget AS department_budget,
        b.location AS department_location,
        b.dept_name AS department_dept_name
    FROM enriched_employees a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
with_windows AS (
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY job_title) AS job_title_avg_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY department_budget DESC, id) AS row_seq,
        FIRST_VALUE(department_dept_name) IGNORE NULLS OVER (PARTITION BY MONTH(hire_date), dept_id ORDER BY budget_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_department_dept_name,
        LAG(prev_budget, 2) OVER (PARTITION BY job_title ORDER BY prev_budget, id) AS prev_prev_budget
    FROM joined
),
enriched_employees_2 AS (
    SELECT
        id,
        full_name,
        dept_id,
        job_title,
        level,
        budget,
        prev_budget,
        department_location,
        department_dept_name,
        job_title_avg_budget,
        prev_prev_budget,
        MAX(budget_avg_3) OVER w_grp AS running_max_budget_avg_3,
        SUM(budget_avg_3) OVER w_grp AS running_sum_budget_avg_3,
        AVG(budget_avg_3) OVER w_grp AS running_avg_budget_avg_3
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY job_title, dept_name ORDER BY budget_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_employees AS (
    SELECT
        id,
        dept_id,
        job_title,
        level,
        prev_budget,
        department_dept_name,
        job_title_avg_budget,
        running_max_budget_avg_3,
        running_sum_budget_avg_3,
        COUNT(running_sum_budget_avg_3) OVER w_grp AS running_count_running_sum_budget_avg_3,
        MAX(running_sum_budget_avg_3) OVER w_grp AS running_max_running_sum_budget_avg_3,
        AVG(running_sum_budget_avg_3) OVER w_grp AS running_avg_running_sum_budget_avg_3,
        LAG(job_title_avg_budget, 2) OVER (PARTITION BY department_dept_name, dept_id ORDER BY running_avg_budget_avg_3, id) AS prev_job_title_avg_budget
    FROM enriched_employees_2
    WINDOW w_grp AS (PARTITION BY department_location, level ORDER BY prev_prev_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
summary AS (
    SELECT
        department_dept_name,
        PERCENTILE_APPROX(running_max_budget_avg_3, 0.95) AS running_max_budget_avg_3_p95,
        SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
        APPROX_PERCENTILE(prev_budget, 0.9) AS prev_budget_p90,
        ROUND(SUM(running_avg_running_sum_budget_avg_3) / NULLIF(COUNT(CASE WHEN job_title = 'Director' THEN 1 END), 0), 2) AS running_avg_running_sum_budget_avg_3_per_director
    FROM with_windows_employees
    GROUP BY ROLLUP (department_dept_name)
)
SELECT
    department_dept_name,
    job_title_list,
    running_avg_running_sum_budget_avg_3_per_director,
    CASE WHEN department_dept_name RLIKE '^(Marketing|HR)$' THEN 'PRIORITY' WHEN department_dept_name RLIKE '^(Engineering|Sales)$' THEN 'EXTENDED' ELSE 'CORE' END AS department_dept_name_zone
FROM summary;
