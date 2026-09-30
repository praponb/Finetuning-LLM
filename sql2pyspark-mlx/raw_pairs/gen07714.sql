WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label
    FROM employees
    WHERE job_title IS DISTINCT FROM 'Engineer'
      AND full_name LIKE 'F%'
),
valid_employees AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        REGEXP_REPLACE(full_name, ' ', '') AS full_name_slug,
        COALESCE(job_title, 'N/A') AS resolved_job_title
    FROM valid
    WHERE hire_date >= DATE '2023-05-22'
      AND job_title IS NOT NULL
),
enriched AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.location,
        d.dept_name
    FROM valid_employees s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
valid_employees_2 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        full_name_slug,
        resolved_job_title,
        location,
        REGEXP_REPLACE(email, ' ', '-') AS email_slug,
        CONCAT(dept_name, ' ', location) AS dept_name_location
    FROM enriched
    WHERE dept_id IN (5, 12, 13, 14)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        location,
        DATE_TRUNC('WEEK', hire_date) AS week_start,
        SIZE(COLLECT_SET(location)) AS num_locations,
        COUNT(DISTINCT manager_id) AS distinct_manager_ids
    FROM valid_employees_2
    GROUP BY location, DATE_TRUNC('WEEK', hire_date)
    HAVING SIZE(COLLECT_SET(location)) >= 1
)
SELECT
    location,
    week_start,
    num_locations,
    CASE WHEN location RLIKE '^(London|Bangkok)$' THEN 'GROUP_A' ELSE 'CORE' END AS location_zone
FROM grouped
ORDER BY location_zone DESC;
