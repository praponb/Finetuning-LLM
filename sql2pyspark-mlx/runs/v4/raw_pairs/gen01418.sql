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
        is_remote,
        level,
        DATE_TRUNC('YEAR', hire_date) AS hire_date_period,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE job_title IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date_period DESC NULLS FIRST, id) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date_period DESC, id) AS row_seq,
        NTILE(3) OVER (ORDER BY salary, id) AS salary_bucket
    FROM ranked
),
enriched AS (
    SELECT
        e.*,
        p.location,
        p.budget
    FROM scored e
    INNER JOIN departments p
        ON e.dept_id = p.id
),
flagged AS (
    SELECT *, CASE WHEN LAG(hire_date_period) OVER (PARTITION BY manager_id ORDER BY hire_date_period, id) IS NULL
                 OR hire_date_period > LAG(hire_date_period) OVER (PARTITION BY manager_id ORDER BY hire_date_period, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY manager_id ORDER BY hire_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT *
FROM (
    SELECT
        id,
        location,
        salary,
        budget
    FROM islands
) src
UNPIVOT (
    reading FOR kpi IN (salary, budget)
);
