WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(location) AS location_upper,
        CASE WHEN location = 'Berlin' THEN 1 ELSE 0 END AS is_berlin
    FROM departments
    WHERE location IS NOT NULL
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.job_title
    FROM filtered s
    JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location, job_title ORDER BY is_berlin, id) AS rank_in_group
    FROM with_latest
    QUALIFY rank_in_group = 1
),
with_latest_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_job_title,
        latest.salary
    FROM deduped s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.manager_id IS NULL
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY rank_in_group, id) AS rn
    FROM eligible
    QUALIFY rn = 1
)
SELECT
    id,
    location,
    budget,
    is_berlin,
    job_title,
    rank_in_group,
    employee_job_title,
    salary
FROM ranked
ORDER BY employee_job_title;
