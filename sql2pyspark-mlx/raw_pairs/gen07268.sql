WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        SUBSTRING(level, 1, 2) level_prefix,
        LENGTH(full_name) full_name_length
    FROM employees
    WHERE level IS NOT NULL
),
filtered_employees AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(full_name, ' - ', level) full_name_level
    FROM filtered
    WHERE job_title IS NOT NULL
      AND level IS DISTINCT FROM 'L5'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY full_name_length, id) AS row_num
    FROM filtered_employees
    QUALIFY row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        full_name_level,
        row_num,
        AVG(full_name_length) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) full_name_length_avg_5_prior,
        AVG(full_name_length) OVER w_ordered full_name_length_avg_3,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY MONTH(hire_date) ORDER BY full_name_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_job_title,
        AVG(full_name_length) OVER (PARTITION BY MONTH(hire_date), dept_id ORDER BY full_name_length, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) full_name_length_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY YEAR(hire_date), manager_id ORDER BY full_name_length DESC) full_name_length_dense_rank
    FROM deduped
    WHERE LENGTH(full_name) > 6
    WINDOW w_ordered AS (PARTITION BY job_title, level ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    dept_id,
    PERCENTILE_APPROX(full_name_length_avg_3, 0.5) AS full_name_length_avg_3_p50,
    APPROX_PERCENTILE(full_name_length_dense_rank, 0.95) AS full_name_length_dense_rank_p95,
    COUNT(*) AS event_count,
    COUNT(DISTINCT dept_id) AS distinct_dept_ids
FROM scored
GROUP BY 1
ORDER BY full_name_length_avg_3_p50
