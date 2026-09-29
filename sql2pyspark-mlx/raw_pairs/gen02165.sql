WITH remaining_keys AS (
    -- Set difference on keys
    SELECT level, job_title
    FROM employees
    EXCEPT DISTINCT
    SELECT level, job_title
    FROM employees
    WHERE manager_id = 13
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN remaining_keys k ON s.level = k.level AND s.job_title = k.job_title
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.full_name,
        e.manager_id,
        e.salary,
        e.is_remote,
        p2.full_name AS employee_full_name
    FROM verified e
    LEFT JOIN employees p2
        ON e.manager_id <=> p2.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_dense_rank,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share_2,
        DENSE_RANK() OVER (PARTITION BY manager_id ORDER BY salary ASC) AS salary_dense_rank_2
    FROM enriched
    WHERE full_name NOT LIKE 'Dav%'
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.full_name,
        a.manager_id,
        a.is_remote,
        a.salary_share_2,
        a.salary_dense_rank_2,
        b.full_name AS employee_full_name_2
    FROM metrics a
    LEFT JOIN employees b
        ON a.manager_id <=> b.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        manager_id,
        is_remote,
        salary_dense_rank_2,
        employee_full_name_2,
        ROUND(salary_share_2 * 1.07, 2) AS salary_share_2_scaled,
        COALESCE(salary_share_2, 0) AS salary_share_2_filled
    FROM with_dims
    WHERE salary_share_2 >= 0.35
)
-- Grouped metrics
SELECT
    CASE WHEN salary_share_2_scaled >= 87 THEN 'HIGH' ELSE 'LOW' END AS salary_share_2_scaled_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    APPROX_PERCENTILE(salary_share_2_scaled, ARRAY(0.25, 0.5, 0.75)) AS salary_share_2_scaled_quartiles,
    APPROX_PERCENTILE(salary_share_2_filled, ARRAY(0.25, 0.5, 0.75)) AS salary_share_2_filled_quartiles,
    APPROX_PERCENTILE(salary_dense_rank_2, 0.5) AS salary_dense_rank_2_p50,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids_2
FROM filtered
GROUP BY CASE WHEN salary_share_2_scaled >= 87 THEN 'HIGH' ELSE 'LOW' END
HAVING APPROX_PERCENTILE(salary_dense_rank_2, 0.5) >= 0
ORDER BY distinct_manager_ids ASC NULLS LAST
