WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY hire_date DESC NULLS LAST, id) = 1
),
enriched AS (
    -- Attach departments attributes
    SELECT a.*, p.dept_name
    FROM ranked a
    LEFT JOIN departments p
        ON a.dept_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        manager_id,
        is_remote,
        AVG(salary) OVER (PARTITION BY YEAR(hire_date), job_title ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior,
        MIN(salary) OVER w_ordered AS manager_id_min_salary
    FROM enriched
    WHERE dept_id IN (12, 32)
    WINDOW w_ordered AS (PARTITION BY manager_id)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(manager_id_min_salary) OVER w_part AS running_max_manager_id_min_salary,
        COUNT(manager_id_min_salary) OVER w_part AS running_count_manager_id_min_salary,
        SUM(manager_id_min_salary) OVER w_part AS running_sum_manager_id_min_salary
    FROM metrics
    WINDOW w_part AS (PARTITION BY manager_id ORDER BY manager_id_min_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.*,
        p.job_title,
        p.level,
        p.full_name
    FROM scored a
    LEFT JOIN employees p
        ON a.manager_id <=> p.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT manager_id, job_title
    FROM with_dims
    INTERSECT
    SELECT manager_id, job_title
    FROM employees
    WHERE dept_id IN (10, 29, 34)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id AND s.job_title = k.job_title
),
metrics_employees AS (
    -- Running and moving aggregates
    SELECT *, COUNT(manager_id_min_salary) OVER w_running AS level_count_manager_id_min_salary
    FROM reconciled
    WINDOW w_running AS (PARTITION BY level)
)
-- Join a dimension and aggregate
SELECT
    dim.level AS employee_level,
    dim.job_title AS employee_job_title,
    COUNT(*) AS rows_total,
    ROUND(AVG(src.level_count_manager_id_min_salary), 2) AS avg_level_count_manager_id_min_salary
FROM metrics_employees src
INNER JOIN employees dim ON src.manager_id = dim.id
GROUP BY dim.level, dim.job_title
ORDER BY rows_total NULLS FIRST
