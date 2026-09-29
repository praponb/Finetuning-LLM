WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        full_name,
        job_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary, id) AS dedup_rank
        FROM employees
    ) x
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        REGEXP_REPLACE(full_name, ' ', '_') AS full_name_slug,
        SUBSTRING(job_title, 1, 4) AS job_title_prefix,
        COALESCE(job_title, 'DIRECT') AS job_title_filled
    FROM ranked
    WHERE job_title = 'Manager'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        job_title,
        full_name_slug,
        job_title_prefix,
        job_title_filled,
        SUBSTRING(full_name, 1, 3) AS full_name_prefix,
        COALESCE(job_title, 'NONE') AS resolved_job_title
    FROM cleaned
    WHERE job_title IS NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        job_title,
        job_title_prefix,
        job_title_filled,
        full_name_prefix,
        resolved_job_title,
        LOWER(job_title) AS job_title_lower,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title_2
    FROM filtered
    WHERE job_title IS DISTINCT FROM 'Manager'
      AND job_title = 'Director'
),
base_employees AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(job_title) AS job_title_upper
    FROM base
    WHERE job_title IS NOT NULL
),
cleaned_employees AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        job_title,
        job_title_prefix,
        job_title_filled,
        resolved_job_title,
        resolved_job_title_2,
        job_title_upper,
        LOWER(full_name) AS full_name_lower,
        LENGTH(full_name) AS full_name_length,
        COALESCE(job_title, 'UNKNOWN') AS job_title_clean
    FROM base_employees
    WHERE job_title IS NOT NULL
      AND LENGTH(full_name) < 14
)
-- Grouped metrics
SELECT
    job_title,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(DISTINCT job_title) AS distinct_job_titles,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(full_name_length, 0.5) AS full_name_length_p50
FROM cleaned_employees
GROUP BY job_title;
