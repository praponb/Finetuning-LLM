WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        TRIM(full_name) AS clean_full_name
    FROM employees
    WHERE manager_id IN (3, 12, 25)
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY salary, id) = 1
),
valid AS (
    SELECT
        *,
        UPPER(full_name) AS full_name_upper,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM deduped
    WHERE dept_id IN (15, 22, 23, 26)
),
joined AS (
    SELECT
        a.id,
        a.email,
        a.dept_id,
        a.job_title,
        a.salary,
        a.full_name_upper,
        a.job_title_clean,
        p.dept_name
    FROM valid a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
enriched AS (
    SELECT
        *,
        COUNT(salary) OVER w_running AS last3_count_salary,
        MIN(salary) OVER w_running AS last3_min_salary,
        MAX(salary) OVER w_running AS last3_max_salary,
        RANK() OVER (PARTITION BY dept_id ORDER BY salary) AS salary_rank
    FROM joined
    WHERE LOWER(email) LIKE '%vy@%'
    WINDOW w_running AS (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_employees AS (
    SELECT
        id,
        email,
        dept_id,
        job_title,
        full_name_upper,
        dept_name,
        last3_count_salary,
        last3_min_salary,
        last3_max_salary,
        MAX(salary) OVER w_hist AS last4_max_salary,
        COUNT(salary) OVER w_hist AS last4_count_salary,
        AVG(salary) OVER w_hist AS last4_avg_salary,
        SUM(salary) OVER w_hist AS last4_sum_salary
    FROM enriched
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY last3_max_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM enriched_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY last4_avg_salary DESC, id) = 1
)
SELECT
    dim.dept_name AS department_dept_name,
    src.job_title,
    COUNT(*) AS cnt,
    MAX(src.last4_sum_salary) AS max_last4_sum_salary,
    AVG(src.last4_avg_salary) AS avg_last4_avg_salary,
    GROUPING_ID() AS level_id
FROM latest src
JOIN departments dim ON src.dept_id = dim.id
GROUP BY ROLLUP (dim.dept_name, src.job_title);
