WITH eligible AS (
    SELECT s.*
    FROM departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.full_name IS NOT NULL
        )
)
SELECT
    id,
    dept_name,
    location,
    budget
FROM eligible;
