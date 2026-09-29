WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) = 1
)
SELECT
    id,
    email,
    dept_id,
    salary,
    hire_date,
    is_remote,
    level
FROM deduped
ORDER BY id NULLS LAST;
