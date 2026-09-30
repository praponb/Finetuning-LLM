WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', full_name, job_title) full_name_job_title_key
    FROM employees
    WHERE level IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date, id) = 1
),
enriched AS (
    -- Attach employees attributes
    SELECT a.*, d.job_title AS employee_job_title
    FROM ranked a
    LEFT JOIN employees d
        ON a.manager_id = d.id
),
ranked_employees AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY employee_job_title ORDER BY hire_date DESC, id) AS row_num
    FROM enriched
    QUALIFY row_num = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY level ORDER BY salary DESC) salary_cume_dist,
        AVG(salary) OVER (ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) salary_avg_3_prior
    FROM ranked_employees
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', hire_date) AS activity_month,
        SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary,
        COUNT_IF(level = 'L4') AS l4_events,
        PERCENTILE_APPROX(salary_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS salary_avg_3_prior_quartiles,
        COUNT(DISTINCT dept_id) AS distinct_dept_ids
    FROM scored
    GROUP BY 1
    HAVING distinct_dept_ids < 1
)
SELECT
    activity_month,
    designer_salary,
    l4_events,
    salary_avg_3_prior_quartiles
FROM summary
ORDER BY l4_events DESC, designer_salary
