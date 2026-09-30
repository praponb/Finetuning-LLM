WITH kept_keys AS (
    -- Set intersection on keys
    SELECT dept_name, id
    FROM departments
    INTERSECT ALL
    SELECT dept_name, id
    FROM departments
    WHERE location = 'Tokyo'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM departments s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name AND s.id = k.id
)
SELECT
    id,
    dept_name,
    budget,
    CASE WHEN dept_name RLIKE '^(Engineering|Sales)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS dept_name_zone
FROM verified
ORDER BY dept_name_zone DESC NULLS LAST;
