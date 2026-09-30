WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), hire_date) / 7 AS weeks_since_hire_date,
        SUBSTRING(level, 1, 3) AS level_prefix,
        COALESCE(level, 'UNKNOWN') AS level_filled
    FROM employees
    WHERE level = 'L2'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(job_title, 'DIRECT') AS job_title_clean
    FROM filtered
    WHERE hire_date >= TO_DATE('2023-03-08')
      AND full_name LIKE 'Car%'
),
joined AS (
    -- Attach employees attributes
    SELECT
        s.id,
        s.full_name,
        s.manager_id,
        s.hire_date,
        s.is_remote,
        s.job_title_clean,
        e.job_title
    FROM base s
    LEFT JOIN employees e
        ON s.manager_id = e.id
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY hire_date, id) AS rn
        FROM joined
    ) t
    WHERE rn = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        full_name,
        job_title
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY rn DESC, id) AS rn_2
        FROM deduped
    ) x
    WHERE rn_2 = 1
)
-- Grouped metrics
SELECT
    job_title,
    COUNT_IF(job_title = 'Analyst') AS analyst_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles
FROM latest
GROUP BY job_title
ORDER BY analyst_events DESC NULLS FIRST
