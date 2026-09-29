WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        IF(is_remote, 'Yes', 'No') is_remote_label,
        COALESCE(job_title, 'UNKNOWN') job_title_filled
    FROM employees
    WHERE manager_id IS NOT NULL
      AND dept_id IN (9, 14, 25)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        COUNT(salary) OVER (PARTITION BY manager_id, level ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_salary,
        AVG(salary) OVER (PARTITION BY manager_id, level ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_salary,
        SUM(salary) OVER (PARTITION BY manager_id, level ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_salary,
        MAX(salary) OVER (PARTITION BY manager_id, level ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_salary,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date), dept_id ORDER BY salary DESC) salary_cume_dist
    FROM cleaned
    WHERE job_title IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(last3_avg_salary) OVER (ORDER BY salary_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last3_avg_salary,
        DENSE_RANK() OVER w_part last3_sum_salary_dense_rank,
        AVG(last3_max_salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last3_max_salary_avg_6
    FROM with_windows
    WINDOW w_part AS (ORDER BY last3_sum_salary)
),
with_windows_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(last3_sum_salary_dense_rank, TRUE) OVER (PARTITION BY dept_id ORDER BY last3_max_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last3_sum_salary_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY job_title ORDER BY last3_count_salary DESC NULLS LAST) last3_count_salary_percent_rank,
        LAG(last3_max_salary, 1) OVER (PARTITION BY manager_id ORDER BY last3_max_salary_avg_6, id) prev_last3_max_salary,
        MIN(last3_count_salary) OVER (PARTITION BY job_title) job_title_min_last3_count_salary,
        COUNT(last3_count_salary) OVER (PARTITION BY dept_id, job_title) dept_id_count_last3_count_salary
    FROM enriched
),
with_windows_employees_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        salary,
        last3_avg_salary,
        last3_sum_salary,
        last3_max_salary,
        salary_cume_dist,
        last3_sum_salary_dense_rank,
        last_known_last3_sum_salary_dense_rank,
        last3_count_salary_percent_rank,
        prev_last3_max_salary,
        dept_id_count_last3_count_salary,
        LAST_VALUE(last3_avg_salary) IGNORE NULLS OVER (PARTITION BY dept_id, job_title ORDER BY dept_id_count_last3_count_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_last3_avg_salary,
        FIRST_VALUE(last3_count_salary) IGNORE NULLS OVER (PARTITION BY dept_id, job_title ORDER BY last3_sum_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_last3_count_salary,
        COUNT(salary) OVER (PARTITION BY manager_id, job_title) manager_id_count_salary,
        AVG(last3_sum_salary) OVER (ORDER BY running_last3_avg_salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) last3_sum_salary_avg_3_prior
    FROM with_windows_employees
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT e.*, d.budget
    FROM with_windows_employees_2 e
    INNER JOIN departments d
        ON e.dept_id = d.id
),
enriched_employees_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(last3_avg_salary) OVER (PARTITION BY dept_id ORDER BY last3_sum_salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_last3_avg_salary,
        MIN(last3_avg_salary) OVER (PARTITION BY dept_id ORDER BY last3_sum_salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_last3_avg_salary,
        SUM(last3_avg_salary) OVER (PARTITION BY dept_id ORDER BY last3_sum_salary, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_last3_avg_salary
    FROM enriched_employees
)
-- Join a dimension and aggregate
SELECT
    d.location,
    COUNT(*) AS cnt,
    ROUND(SUM(src.last3_sum_salary_dense_rank), 2) AS total_last3_sum_salary_dense_rank
FROM enriched_employees_2 src
INNER JOIN departments d ON src.dept_id = d.id
GROUP BY ROLLUP (d.location)
