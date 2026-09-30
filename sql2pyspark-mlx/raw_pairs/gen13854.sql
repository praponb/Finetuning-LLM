WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(hire_date, 1) AS hire_date_plus_months
    FROM employees
    WHERE hire_date > DATE '2023-06-23'
      AND job_title IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        is_remote,
        level,
        AVG(salary) OVER (PARTITION BY manager_id) AS manager_id_avg_salary,
        ROW_NUMBER() OVER (PARTITION BY MONTH(hire_date_plus_months), dept_id ORDER BY hire_date_plus_months DESC, id) AS row_seq,
        LAG(salary, 1, 0) OVER (PARTITION BY YEAR(hire_date_plus_months), level ORDER BY hire_date_plus_months, id) AS prev_salary
    FROM valid
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(manager_id_avg_salary) OVER w_recent AS last3_max_manager_id_avg_salary,
        MIN(manager_id_avg_salary) OVER w_recent AS last3_min_manager_id_avg_salary,
        AVG(manager_id_avg_salary) OVER w_recent AS last3_avg_manager_id_avg_salary,
        AVG(row_seq) OVER (PARTITION BY manager_id ORDER BY manager_id_avg_salary, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY prev_salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(email) AS clean_email
    FROM enriched
    WHERE level IS NOT NULL
)
-- Multi-level aggregation
SELECT
    manager_id,
    level,
    ROUND(SUM(last3_max_manager_id_avg_salary) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) AS last3_max_manager_id_avg_salary_per_l5,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    AVG(last3_max_manager_id_avg_salary) AS avg_last3_max_manager_id_avg_salary,
    SUM(CASE WHEN level = 'L4' THEN last3_min_manager_id_avg_salary ELSE 0 END) AS l4_last3_min_manager_id_avg_salary
FROM base
GROUP BY manager_id, level
HAVING ROUND(SUM(last3_max_manager_id_avg_salary) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) > 0
ORDER BY l1_count DESC
