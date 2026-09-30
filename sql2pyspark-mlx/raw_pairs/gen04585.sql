WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        LOWER(full_name) full_name_lower,
        COALESCE(level, 'Unknown') level_filled,
        COALESCE(job_title, 'N/A') job_title_filled
    FROM employees
    WHERE job_title IS NOT NULL
),
with_dims AS (
    SELECT
        a.id,
        a.manager_id,
        a.job_title,
        a.salary,
        a.is_remote,
        a.level_filled,
        a.job_title_filled,
        e.salary AS employee_salary,
        e.level AS employee_level
    FROM valid a
    JOIN employees e
        ON a.manager_id = e.id
),
metrics AS (
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) salary_avg_2_prior,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY employee_salary, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) salary_avg_2_prior_2,
        LEAD(salary, 1) OVER (ORDER BY employee_salary, id) next_salary
    FROM with_dims
    WHERE manager_id IN (29, 38)
),
scored AS (
    SELECT
        id,
        manager_id,
        salary,
        level_filled,
        employee_salary,
        salary_avg_2_prior_2,
        next_salary,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_salary,
        MAX(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_salary,
        MIN(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_salary,
        SUM(salary) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_salary,
        LAG(next_salary, 1, 0) OVER (ORDER BY salary_avg_2_prior_2, id) prev_next_salary
    FROM metrics
),
valid_employees AS (
    SELECT
        *,
        CASE WHEN employee_salary >= 156000 THEN 'High' WHEN employee_salary >= 48000 THEN 'Medium' ELSE 'Low' END employee_salary_level,
        ROUND(salary_avg_2_prior_2, 2) salary_avg_2_prior_2_rounded
    FROM scored
    WHERE manager_id IS NOT NULL
),
enriched AS (
    SELECT
        s.id,
        s.manager_id,
        s.level_filled,
        s.employee_salary,
        s.salary_avg_2_prior_2,
        s.running_avg_salary,
        s.running_max_salary,
        s.running_min_salary,
        s.running_sum_salary,
        s.prev_next_salary,
        d.job_title
    FROM valid_employees s
    LEFT JOIN employees d
        ON s.manager_id = d.id
)
SELECT
    p.job_title AS employee_job_title,
    src.job_title AS employee_job_title,
    COUNT(*) AS num_rows,
    ROUND(AVG(src.salary_avg_2_prior_2), 2) AS avg_salary_avg_2_prior_2,
    MAX(src.prev_next_salary) AS max_prev_next_salary
FROM enriched src
LEFT JOIN employees p ON src.manager_id = p.id
GROUP BY CUBE (p.job_title, src.job_title)
