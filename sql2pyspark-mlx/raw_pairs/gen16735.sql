WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        COALESCE(full_name, 'Unknown') AS full_name_filled,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM employees
    WHERE hire_date >= TO_DATE('2023-09-10')
),
joined AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.email,
        src.dept_id,
        src.salary,
        src.full_name_filled,
        d.budget,
        d.location,
        d.dept_name
    FROM filtered src
    JOIN departments d
        ON src.dept_id = d.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_id,
        full_name_filled,
        budget,
        location
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY budget DESC, id) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, location ORDER BY budget NULLS FIRST, id) AS row_num
        FROM deduped
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.dept_id,
        d.location AS department_location,
        d.budget AS department_budget,
        d.dept_name
    FROM ranked src
    LEFT JOIN departments d
        ON src.dept_id = d.id
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN department_budget >= 3364000 THEN 'HIGH' ELSE 'LOW' END AS department_budget_tier,
        dept_id,
        COUNT(*) FILTER (WHERE department_location = 'Tokyo') AS tokyo_rows,
        COUNT(*) FILTER (WHERE dept_name = 'Finance') AS finance_rows
    FROM enriched
    GROUP BY GROUPING SETS ((CASE WHEN department_budget >= 3364000 THEN 'HIGH' ELSE 'LOW' END, dept_id), (CASE WHEN department_budget >= 3364000 THEN 'HIGH' ELSE 'LOW' END))
)
SELECT
    department_budget_tier,
    dept_id,
    tokyo_rows,
    finance_rows
FROM totals
