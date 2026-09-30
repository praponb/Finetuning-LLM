WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(job_title) AS clean_job_title,
        DATE_ADD(hire_date, 86) AS hire_date_plus_86d
    FROM employees
    WHERE hire_date >= DATE '2023-08-11'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(job_title, TRUE) OVER (ORDER BY hire_date_plus_86d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
    FROM filtered
),
joined AS (
    -- Attach employees attributes
    SELECT
        e.*,
        b.email AS employee_email,
        b.full_name AS employee_full_name
    FROM metrics e
    LEFT JOIN employees b
        ON e.manager_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        level,
        dept_id,
        job_title,
        salary_avg_3_prior
    FROM joined
) src
PIVOT (
    SUM(salary_avg_3_prior) AS total
    FOR job_title IN ('Director', 'Designer')
);
