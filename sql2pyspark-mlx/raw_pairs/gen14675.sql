WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('month', hire_date) AS hire_date_period
    FROM employees
    WHERE UPPER(TRIM(email)) = 'EMMA@OUTLOOK.COM'
      AND level IS DISTINCT FROM 'L4'
)
-- Ranking and gap features
SELECT
    id,
    email,
    manager_id,
    job_title,
    hire_date,
    level,
    hire_date_period,
    ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS row_seq,
    AVG(salary) OVER (PARTITION BY manager_id ORDER BY CAST(hire_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS salary_avg_7d,
    ROW_NUMBER() OVER (PARTITION BY YEAR(hire_date), job_title ORDER BY salary, id) AS event_seq,
    ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share
FROM cleaned;
