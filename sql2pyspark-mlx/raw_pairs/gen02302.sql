WITH valid AS (
    SELECT *, UPPER(dept_name) AS dept_name_upper
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name = 'HR'
)
SELECT
    id,
    dept_name,
    budget,
    dept_name_upper,
    RANK() OVER (PARTITION BY dept_name, location ORDER BY budget DESC NULLS FIRST) AS budget_rank,
    NTILE(10) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS budget_bucket
FROM valid
