WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        email,
        manager_id,
        job_title,
        salary,
        level,
        TRIM(job_title) AS clean_job_title,
        CASE WHEN is_remote THEN 'Yes' ELSE 'No' END AS is_remote_label,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM employees
    WHERE hire_date > DATE '2023-05-22'
      AND manager_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY salary DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(salary, 2) OVER (ORDER BY latest_rank, id) AS prev_salary,
        SUM(salary) OVER (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        DENSE_RANK() OVER (PARTITION BY level, manager_id ORDER BY salary DESC) AS salary_dense_rank,
        LAG(salary, 1, 0) OVER (PARTITION BY manager_id ORDER BY latest_rank, id) AS prev_salary_2,
        RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_rank
    FROM ranked
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        clean_job_title,
        resolved_job_title,
        prev_salary_2,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        SUM(salary_dense_rank) OVER (PARTITION BY level ORDER BY salary_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_dense_rank,
        AVG(salary_rank) OVER w_recent AS salary_rank_avg_3_prior
    FROM scored
    WHERE salary_dense_rank <> 4
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY salary_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        level,
        clean_job_title,
        resolved_job_title,
        cumulative_salary_dense_rank,
        ROUND(prev_salary_2 / SUM(prev_salary_2) OVER (PARTITION BY level), 4) AS prev_salary_2_share,
        CUME_DIST() OVER (PARTITION BY level ORDER BY prev_salary_2 DESC NULLS LAST) AS prev_salary_2_cume_dist,
        CUME_DIST() OVER (PARTITION BY level ORDER BY cumulative_salary_dense_rank DESC NULLS LAST) AS cumulative_salary_dense_rank_cume_dist,
        AVG(salary_rank_avg_3_prior) OVER (PARTITION BY level ORDER BY salary_rank_avg_3_prior, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS salary_rank_avg_3_prior_avg_2
    FROM metrics
)
SELECT
    id,
    level,
    clean_job_title,
    resolved_job_title,
    cumulative_salary_dense_rank,
    prev_salary_2_share,
    CASE WHEN level RLIKE '^(L1|L5)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS level_zone
FROM with_windows;
