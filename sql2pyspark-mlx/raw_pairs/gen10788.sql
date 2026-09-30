WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        CAST(is_remote AS INT) is_remote_flag
    FROM employees
    WHERE hire_date > DATE '2023-07-08'
      AND LENGTH(email) >= 11
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        manager_id,
        is_remote,
        level,
        is_remote_flag,
        SUBSTRING(job_title, 1, 4) job_title_prefix,
        COALESCE(level, 'unassigned') level_filled
    FROM valid
    WHERE manager_id IS NOT NULL
      AND full_name LIKE 'FR%'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(is_remote_flag, 2) OVER w_grp next_is_remote_flag,
        MIN(is_remote_flag) OVER (PARTITION BY level) level_min_is_remote_flag,
        STDDEV_SAMP(is_remote_flag) OVER (PARTITION BY manager_id) manager_id_stddev_samp_is_remote_flag,
        SUM(is_remote_flag) OVER (PARTITION BY manager_id ORDER BY is_remote_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_remote_flag
    FROM base
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY is_remote_flag, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        manager_id,
        is_remote,
        job_title_prefix,
        level_filled,
        next_is_remote_flag,
        level_min_is_remote_flag,
        LAG(next_is_remote_flag) OVER (PARTITION BY manager_id ORDER BY level_min_is_remote_flag, id) prev_next_is_remote_flag,
        LEAD(is_remote_flag) OVER (PARTITION BY manager_id ORDER BY level_min_is_remote_flag, id) next_is_remote_flag_2,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY level_min_is_remote_flag, id) seq_no
    FROM enriched
    WHERE full_name IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    manager_id,
    CASE WHEN level_min_is_remote_flag >= 78 THEN 'HIGH' ELSE 'LOW' END AS level_min_is_remote_flag_tier,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids_2,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(next_is_remote_flag) AS max_next_is_remote_flag
FROM scored
GROUP BY manager_id, CASE WHEN level_min_is_remote_flag >= 78 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_manager_ids NULLS FIRST
