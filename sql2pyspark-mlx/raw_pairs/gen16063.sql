WITH valid AS (
    SELECT *, CEIL(budget) AS budget_whole
    FROM departments
    WHERE location IS NOT NULL
)
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM employees ch
        WHERE ch.dept_id = s.id
          AND ch.job_title IN ('Engineer', 'Analyst')
    );
