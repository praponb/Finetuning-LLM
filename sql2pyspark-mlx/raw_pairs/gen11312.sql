WITH eligible AS (
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title NOT IN ('Designer', 'Intern')
        )
)
SELECT
    id,
    dept_name,
    location
FROM eligible
ORDER BY dept_name DESC NULLS FIRST;
