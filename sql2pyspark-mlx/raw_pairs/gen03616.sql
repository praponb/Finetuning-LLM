WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(level) AS clean_level,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM employees
    WHERE hire_date >= DATE '2023-06-13'
      AND dept_id = 37
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC, id) AS rn
    FROM base
    QUALIFY rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title,
        salary,
        is_remote,
        level,
        clean_level,
        rn,
        LAG(salary, 1) OVER (PARTITION BY job_title ORDER BY rn, id) AS prev_salary,
        LAST_VALUE(level) IGNORE NULLS OVER (PARTITION BY dept_id, job_title ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM ranked
    WHERE dept_id IN (13, 33)
),
ranked_employees AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY rn, id) AS rn_2
    FROM metrics
    QUALIFY rn_2 = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY prev_salary DESC NULLS LAST, id) AS rn_3
    FROM ranked_employees
    QUALIFY rn_3 = 1
)
-- Grouped metrics
SELECT
    CASE WHEN prev_salary >= 130000 THEN 'LARGE' ELSE 'SMALL' END AS prev_salary_tier,
    CASE WHEN salary >= 121000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE level = 'L5') AS l5_rows,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list
FROM deduped
GROUP BY 1, 2
ORDER BY salary_tier DESC
