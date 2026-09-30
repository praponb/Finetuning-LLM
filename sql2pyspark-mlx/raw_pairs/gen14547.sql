WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC NULLS LAST) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.budget,
        d.location,
        d.dept_name
    FROM ranked e
    LEFT JOIN departments d
        ON e.dept_id = d.id
    WHERE d.budget > 1264000
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        is_remote,
        level,
        rank_in_group,
        location,
        dept_name,
        NTILE(5) OVER (PARTITION BY MONTH(hire_date) ORDER BY salary DESC, id) AS salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title, manager_id), 4) AS salary_share,
        AVG(salary) OVER (PARTITION BY manager_id ORDER BY rank_in_group, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3
    FROM joined
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        manager_id,
        job_title,
        is_remote,
        level,
        location,
        salary_bucket,
        salary_avg_3,
        LAG(salary_bucket) OVER (PARTITION BY level, location ORDER BY salary_avg_3, id) AS prev_salary_bucket,
        LEAD(salary_share) OVER (PARTITION BY level, location ORDER BY salary_avg_3, id) AS next_salary_share,
        ROW_NUMBER() OVER (PARTITION BY level, location ORDER BY salary_avg_3, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary_share DESC, id) AS event_seq
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_salary_share >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_salary_share_tier,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    ROUND(SUM(salary_avg_3) / NULLIF(COUNT(CASE WHEN location = 'London' THEN 1 END), 0), 2) AS salary_avg_3_per_london,
    APPROX_PERCENTILE(salary_bucket, ARRAY(0.25, 0.5, 0.75)) AS salary_bucket_quartiles
FROM metrics
GROUP BY 1
ORDER BY salary_avg_3_per_london;
