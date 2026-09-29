WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        TRIM(job_title) AS clean_job_title
    FROM employees
    WHERE salary > 60000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_id,
        job_title,
        clean_job_title,
        dedup_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date ASC NULLS FIRST, id ASC) AS row_num
        FROM ranked
    ) t
    WHERE row_num = 1
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.dept_id,
        s.job_title,
        s.clean_job_title,
        p.location
    FROM latest s
    INNER JOIN departments p
        ON s.dept_id = p.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(job_title) AS job_title_length,
        COALESCE(job_title, 'N/A') AS job_title_clean
    FROM with_dims
    WHERE dept_id = 6
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        clean_job_title,
        job_title_clean,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY dept_id ORDER BY job_title_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        MIN(job_title_length) OVER (PARTITION BY dept_id) AS dept_id_min_job_title_length
    FROM cleaned
    WHERE job_title_length >= 47
)
-- Multi-level aggregation
SELECT
    CASE WHEN dept_id_min_job_title_length >= 63 THEN 'LARGE' ELSE 'SMALL' END AS dept_id_min_job_title_length_tier,
    ROUND(AVG(dept_id_min_job_title_length), 2) AS avg_dept_id_min_job_title_length,
    APPROX_PERCENTILE(dept_id_min_job_title_length, ARRAY(0.25, 0.5, 0.75)) AS dept_id_min_job_title_length_quartiles
FROM with_windows
GROUP BY CASE WHEN dept_id_min_job_title_length >= 63 THEN 'LARGE' ELSE 'SMALL' END
