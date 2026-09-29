WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LOWER(job_title) AS job_title_lower,
        COALESCE(job_title, 'OTHER') AS job_title_filled
    FROM employees
    WHERE hire_date >= DATE '2023-07-20'
)
-- Deduplicate with ROW_NUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id) AS rank_in_group
FROM cleaned
QUALIFY rank_in_group = 1
