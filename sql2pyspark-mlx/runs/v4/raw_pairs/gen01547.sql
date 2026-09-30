WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(dept_name, 1, 2) AS dept_name_prefix,
        ROUND(budget, 0) AS budget_rounded,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_filled
    FROM departments
    WHERE budget <= 2014000
),
remaining_keys AS (
    SELECT id, location
    FROM cleaned
    EXCEPT ALL
    SELECT id, location
    FROM departments
    WHERE budget BETWEEN 1085000 AND 1627000
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
),
kept_keys AS (
    SELECT location, id
    FROM kept_rows
    EXCEPT ALL
    SELECT location, id
    FROM departments
    WHERE dept_name IN ('Marketing', 'Finance', 'Legal')
),
kept_rows_departments AS (
    SELECT s.*
    FROM kept_rows s
    JOIN kept_keys k ON s.location = k.location AND s.id = k.id
),
cleaned_departments AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_prefix,
        ROUND(budget_rounded / budget, 2) AS budget_rounded_per_budget,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM kept_rows_departments
    WHERE dept_name IS NOT NULL
      AND budget_rounded BETWEEN 45 AND 48
),
cleaned_departments_2 AS (
    SELECT *, budget * budget_rounded_per_budget AS budget_times_budget_rounded_per_budget
    FROM cleaned_departments
    WHERE budget >= 2949000
)
SELECT
    dept_name,
    location,
    budget,
    budget_rounded_per_budget,
    location_clean,
    CASE WHEN location RLIKE '^(Chiang Mai|Bangkok)$' THEN 'CORE' ELSE 'PRIORITY' END AS location_zone
FROM cleaned_departments_2
ORDER BY budget, dept_name
