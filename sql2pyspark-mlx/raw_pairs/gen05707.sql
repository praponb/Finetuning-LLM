WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(salary, 1) OVER (PARTITION BY MONTH(hire_date), dept_id ORDER BY salary, id) AS prev_salary,
        AVG(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS salary_avg_5_prior,
        LAG(salary, 1, 0) OVER w_running AS prev_salary_2
    FROM employees
    WINDOW w_running AS (PARTITION BY dept_id ORDER BY salary, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        job_title,
        is_remote,
        level,
        prev_salary,
        prev_salary_2,
        LEAD(prev_salary, 2, 0) OVER (PARTITION BY MONTH(hire_date) ORDER BY salary, id) AS next_prev_salary,
        AVG(salary_avg_5_prior) OVER (ORDER BY salary_avg_5_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5_prior_avg_5,
        SUM(salary_avg_5_prior) OVER (ORDER BY prev_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_avg_5_prior,
        LAST_VALUE(prev_salary, TRUE) OVER (ORDER BY prev_salary_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_salary,
        ROUND(prev_salary_2 / SUM(prev_salary_2) OVER (PARTITION BY MONTH(hire_date), manager_id), 4) AS prev_salary_2_share
    FROM scored
),
kept_keys AS (
    -- Set intersection on keys
    SELECT job_title
    FROM with_windows
    INTERSECT
    SELECT job_title
    FROM employees
    WHERE UPPER(full_name) LIKE '%OB %'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.job_title = k.job_title
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        manager_id,
        job_title,
        level,
        prev_salary,
        prev_salary_2,
        next_prev_salary,
        salary_avg_5_prior_avg_5,
        last_known_prev_salary,
        prev_salary_2_share,
        AVG(prev_salary_2) OVER (PARTITION BY level) AS level_avg_prev_salary_2
    FROM verified
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.*,
        e.salary,
        e.level AS employee_level,
        e.full_name
    FROM metrics a
    LEFT JOIN employees e
        ON a.manager_id = e.id
    WHERE e.email LIKE '%@yahoo.com'
)
-- Grouped metrics
SELECT
    job_title,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE job_title = 'Designer') AS designer_rows
FROM with_dims
GROUP BY job_title;
