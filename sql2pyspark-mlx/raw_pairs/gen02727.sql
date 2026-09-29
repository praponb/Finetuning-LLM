WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        top1.employee_level
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < '2023-09-01'
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_name, id
    FROM kept
    EXCEPT DISTINCT
    SELECT dept_name, id
    FROM departments
    WHERE budget >= 3652000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(dept_name, 'Unknown') AS dept_name_filled
    FROM reconciled
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title = 'Director'
        )
)
SELECT
    id,
    dept_name,
    dept_name_filled,
    CASE WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'EXTENDED' WHEN dept_name RLIKE '^(Marketing|HR)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS dept_name_zone
FROM not_flagged;
