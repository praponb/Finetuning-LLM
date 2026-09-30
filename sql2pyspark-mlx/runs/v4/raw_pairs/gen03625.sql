WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_id, job_title ORDER BY salary) AS dedup_rank
        FROM employees
    ) t
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY dept_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level,
        CUME_DIST() OVER w_hist AS salary_cume_dist
    FROM deduped
    WINDOW w_hist AS (PARTITION BY level ORDER BY salary DESC)
),
joined AS (
    -- Attach employees attributes
    SELECT a.*, d.job_title AS employee_job_title
    FROM with_windows a
    INNER JOIN employees d
        ON a.manager_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(salary_cume_dist) OVER (PARTITION BY dept_id) AS dept_id_min_salary_cume_dist,
        SUM(salary) OVER (PARTITION BY dept_id, manager_id) AS dept_id_sum_salary,
        AVG(salary_cume_dist) OVER (PARTITION BY level, job_title ORDER BY hire_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_cume_dist_avg_5
    FROM joined
)
SELECT
    id,
    full_name,
    dept_id,
    job_title,
    hire_date,
    last_known_level,
    employee_job_title,
    dept_id_min_salary_cume_dist,
    dept_id_sum_salary,
    CASE WHEN job_title RLIKE '^(Manager|Analyst)$' THEN 'CORE' ELSE 'EXTENDED' END AS job_title_zone
FROM enriched;
