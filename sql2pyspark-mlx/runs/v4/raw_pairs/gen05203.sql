WITH joined AS (
    -- Attach employees attributes
    SELECT src.*, p.full_name AS employee_full_name
    FROM employees src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        is_remote,
        level,
        CONCAT_WS('/', full_name, email) AS full_name_email_key,
        COALESCE(job_title, 'NONE') AS job_title_clean
    FROM joined
    WHERE salary <= 133000
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(level) AS clean_level
    FROM base
    WHERE level NOT IN ('L4', 'L1', 'L3')
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, job_title
    FROM valid
    EXCEPT
    SELECT id, job_title
    FROM employees
    WHERE dept_id IN (12, 13, 20, 30)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.id = k.id AND s.job_title = k.job_title
),
joined_employees AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.dept_name,
        d.location
    FROM reconciled src
    LEFT JOIN departments d
        ON src.dept_id = d.id
)
-- Grouped metrics
SELECT
    level,
    dept_name,
    SIZE(COLLECT_SET(location)) AS num_locations,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count,
    COUNT(*) AS event_count,
    GROUPING(level) AS grouping_id
FROM joined_employees
GROUP BY CUBE (level, dept_name)
