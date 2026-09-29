WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level), 4) AS salary_share,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC, id) AS seq_no,
        AVG(salary) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior
    FROM employees
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary_avg_3_prior NULLS FIRST, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        salary_share,
        seq_no,
        salary_avg_3_prior,
        UPPER(level) AS level_upper,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM ranked
    WHERE salary_avg_3_prior > 108791
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        job_title,
        salary_share,
        salary
    FROM base
) src
UNPIVOT (
    reading FOR measure IN (salary_share, salary)
)
