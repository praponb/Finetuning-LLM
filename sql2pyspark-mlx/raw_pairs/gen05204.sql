WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        is_remote,
        level,
        CONCAT(email, ' ', level) AS email_level,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'UNKNOWN') AS resolved_level
    FROM employees
    WHERE level IS NOT NULL
      AND level IS NOT DISTINCT FROM 'L4'
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.full_name,
        a.dept_id,
        a.is_remote,
        a.email_level,
        a.resolved_level,
        b.budget,
        b.location,
        b.dept_name
    FROM valid a
    LEFT JOIN departments b
        ON a.dept_id = b.id
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.id,
        src.full_name,
        src.dept_id,
        src.is_remote,
        src.location,
        src.dept_name,
        b.location AS department_location
    FROM enriched src
    JOIN departments b
        ON src.dept_id = b.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN department_location = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai
    FROM with_dims
    WHERE department_location IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT dept_id, id
    FROM cleaned
    EXCEPT ALL
    SELECT dept_id, id
    FROM employees
    WHERE LOWER(TRIM(email)) = 'david.lee@gmail.com'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.dept_id = k.dept_id AND s.id = k.id
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.location,
        a.department_location,
        a.is_chiang_mai,
        p.budget,
        p.dept_name AS department_dept_name
    FROM verified a
    JOIN departments p
        ON a.dept_id = p.id
)
-- Running and moving aggregates
SELECT *, COUNT(is_chiang_mai) OVER (PARTITION BY location, department_location) AS location_count_is_chiang_mai
FROM joined
WHERE department_dept_name = 'Sales'
