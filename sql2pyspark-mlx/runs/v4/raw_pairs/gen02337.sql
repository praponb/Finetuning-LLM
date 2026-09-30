WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        LOWER(level) AS level_lower,
        ADD_MONTHS(hire_date, 6) AS hire_date_plus_months,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM employees
    WHERE email LIKE '%@gmail.com'
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS rank_in_group
FROM filtered
QUALIFY rank_in_group = 1;
