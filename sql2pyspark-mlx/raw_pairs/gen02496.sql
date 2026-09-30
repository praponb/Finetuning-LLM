WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        DATEDIFF(CURRENT_DATE(), hire_date) AS days_since_hire_date
    FROM employees
    WHERE hire_date >= DATE '2023-05-05'
      AND manager_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY salary DESC, id) AS rn
        FROM base
    ) t
    WHERE rn = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        days_since_hire_date,
        rn
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY rn DESC, id) AS row_num
        FROM deduped
    ) t
    WHERE row_num = 1
),
deduped_employees AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        full_name,
        hire_date,
        rn,
        latest_rank
    FROM deduped_employees
    QUALIFY RANK() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC) = 1
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2
FROM latest
GROUP BY DATE_TRUNC('QUARTER', hire_date), DATE_TRUNC('MONTH', hire_date)
HAVING COUNT(*) >= 1
