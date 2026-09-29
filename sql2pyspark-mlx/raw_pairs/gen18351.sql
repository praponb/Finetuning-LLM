WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        CASE WHEN salary >= 148000 THEN 'High' WHEN salary >= 42000 THEN 'Medium' ELSE 'Low' END AS salary_band,
        COALESCE(job_title, 'OTHER') AS job_title_filled
    FROM employees
    WHERE hire_date > DATE '2023-03-18'
),
with_windows AS (
    SELECT
        id,
        email,
        dept_id,
        salary,
        hire_date,
        salary_band,
        job_title_filled,
        FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date) ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        LAG(salary, 2) OVER (ORDER BY salary, id) AS prev_salary,
        RANK() OVER (ORDER BY salary DESC) AS salary_rank
    FROM cleaned
),
deduped AS (
    SELECT
        id,
        salary,
        hire_date,
        salary_band,
        job_title_filled,
        first_known_job_title
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY dept_id ORDER BY salary_rank DESC) = 1
),
valid AS (
    SELECT *, DATE_TRUNC('week', hire_date) AS hire_date_period
    FROM deduped
    WHERE hire_date <= '2024-10-01'
)
SELECT
    CASE WHEN salary >= 134000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
    PERCENTILE_APPROX(salary, 0.5) AS salary_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(hire_date_period) AS first_hire_date_period
FROM valid
GROUP BY CASE WHEN salary >= 134000 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(salary, 0.5) >= 0;
