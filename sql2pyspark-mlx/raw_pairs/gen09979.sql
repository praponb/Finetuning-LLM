WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        ADD_MONTHS(hire_date, 3) hire_date_plus_months
    FROM employees
    WHERE hire_date >= DATE '2023-04-02'
      AND manager_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        hire_date_plus_months,
        CAST(salary AS INT) salary_int,
        DATE_FORMAT(hire_date_plus_months, 'yyyy-MM-dd') hire_date_plus_months_label,
        COALESCE(job_title, 'DIRECT') job_title_filled
    FROM filtered
    WHERE dept_id IN (27, 31)
      AND job_title IS DISTINCT FROM 'Intern'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        hire_date_plus_months,
        salary_int,
        hire_date_plus_months_label,
        job_title_filled,
        ROUND(salary_int, 1) salary_int_rounded,
        ROUND(salary_int * 0.85, 2) salary_int_adjusted
    FROM base
    WHERE job_title IS NOT DISTINCT FROM 'Intern'
      AND YEAR(hire_date_plus_months) >= 2025
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary_int_adjusted DESC NULLS FIRST, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', hire_date_plus_months) AS year_start,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list_2
FROM deduped
GROUP BY DATE_TRUNC('YEAR', hire_date_plus_months)
HAVING distinct_ids > 1
ORDER BY year_start NULLS LAST;
