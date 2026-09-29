WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(dept_name) AS dept_name_upper,
        COALESCE(dept_name, 'N/A') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.level,
        x.employee_job_title
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.level, c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(budget) OVER w_hist AS next_budget,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM with_last
    WINDOW w_hist AS (PARTITION BY dept_name, level ORDER BY budget, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE budget >= 1660000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        budget - next_budget AS budget_minus_next_budget,
        CONCAT_WS('/', employee_job_title, location) AS employee_job_title_location_key,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM reconciled
    WHERE next_budget < 2251000
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT location
    FROM base
    INTERSECT
    SELECT location
    FROM departments
    WHERE dept_name NOT IN ('Engineering', 'HR')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.location = k.location
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        level,
        next_budget,
        budget
    FROM kept_rows
) src
UNPIVOT (
    reading FOR measure IN (next_budget AS NEXT_BUDGET_VAL, budget AS BUDGET_VAL)
);
