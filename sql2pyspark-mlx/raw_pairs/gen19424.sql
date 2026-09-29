WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(dept_name) dept_name_length
    FROM departments
    WHERE location IS NOT NULL
      AND budget >= 1517000
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date <= '2025-01-14'
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT location, id
    FROM eligible
    EXCEPT
    SELECT location, id
    FROM departments
    WHERE location <> 'Tokyo'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.location = k.location AND s.id = k.id
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title NOT IN ('Manager', 'Designer')
        )
),
eligible_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.email LIKE '%@gmail.com'
        )
)
SELECT id, dept_name
FROM eligible_departments
ORDER BY id ASC;
