WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(dept_name)) AS dept_name_title,
        UPPER(location) AS location_upper
    FROM departments
    WHERE location IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        location,
        budget,
        dept_name_title,
        location_upper,
        CAST(budget AS INT) AS budget_int,
        FLOOR(budget) AS budget_whole,
        COALESCE(location, 'UNKNOWN') AS location_clean
    FROM filtered
    WHERE dept_name IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.level,
        x.job_title
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.level, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(budget_int * 0.9, 2) AS adjusted_budget_int
    FROM latest_match
    WHERE job_title IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY budget_int DESC NULLS LAST, id) AS dedup_rank
        FROM base
    ) ranked
    WHERE dedup_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT DISTINCT
    SELECT id
    FROM departments
    WHERE budget <= 1259000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    location,
    budget,
    dept_name_title,
    budget_int,
    budget_whole,
    location_clean,
    level,
    job_title
FROM reconciled
ORDER BY level NULLS FIRST
