WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(budget) AS budget_whole,
        UPPER(dept_name) AS dept_name_upper
    FROM departments
    WHERE budget BETWEEN 1414000 AND 3210000
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id = 30
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.full_name LIKE '%Olivi%'
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        location,
        budget,
        budget_whole,
        dept_name_upper,
        LENGTH(location) AS location_length,
        ROUND(budget_whole, 2) AS budget_whole_rounded
    FROM kept
    WHERE location IN ('New York', 'Singapore', 'Berlin')
),
cleaned_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        location,
        budget,
        budget_whole,
        dept_name_upper,
        location_length,
        budget_whole * budget AS budget_whole_times_budget,
        COALESCE(location, 'DIRECT') AS location_clean
    FROM filtered
    WHERE location IS NOT NULL
      AND budget_whole = 28
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        location,
        budget_whole_times_budget,
        budget
    FROM cleaned_departments
) src
UNPIVOT (
    value FOR measure IN (budget_whole_times_budget AS BUDGET_WHOLE_TIMES_BUDGET, budget AS BUDGET_VAL)
);
