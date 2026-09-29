WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(job_title, '/', email) AS job_title_email,
        COALESCE(level, 'OTHER') AS level_filled
    FROM employees
    WHERE level IS NOT NULL
      AND job_title IS NOT DISTINCT FROM 'Analyst'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        is_remote,
        level,
        job_title_email,
        level_filled,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(hire_date), manager_id ORDER BY salary DESC, id) AS row_seq,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share
    FROM filtered
    WHERE salary <= 106000
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        email,
        is_remote,
        level,
        job_title_email,
        level_filled,
        salary_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary, id) AS rank_in_group
        FROM with_windows
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_remote,
        level,
        level_filled,
        NTILE(5) OVER (PARTITION BY level ORDER BY salary_share, id) AS salary_share_bucket,
        MIN(salary_share) OVER (PARTITION BY level) AS level_min_salary_share,
        CUME_DIST() OVER (ORDER BY salary_share NULLS FIRST) AS salary_share_cume_dist,
        SUM(salary_share) OVER (PARTITION BY level ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_share
    FROM ranked
),
ranked_employees AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary_share_cume_dist DESC NULLS FIRST, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        is_remote,
        level
    FROM ranked_employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level ORDER BY level_min_salary_share, id) = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT level, id
    FROM deduped
    INTERSECT ALL
    SELECT level, id
    FROM employees
    WHERE dept_id IN (7, 15, 16, 26)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    level,
    COUNT_IF(level = 'L2') AS l2_events,
    COUNT(*) FILTER (WHERE level = 'L4') AS l4_rows,
    COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
    SIZE(COLLECT_SET(level)) AS num_levels
FROM kept_rows
GROUP BY 1
ORDER BY num_levels DESC
