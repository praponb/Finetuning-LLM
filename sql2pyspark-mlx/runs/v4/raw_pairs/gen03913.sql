WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) salary_avg_5_prior,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) salary_share,
        STDDEV_SAMP(salary) OVER (PARTITION BY dept_id, level) dept_id_stddev_samp_salary
    FROM employees
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_in_group
        FROM metrics
    ) t
    WHERE rank_in_group = 1
),
joined AS (
    -- Attach employees attributes
    SELECT s.*, e.job_title AS employee_job_title
    FROM deduped s
    INNER JOIN employees e
        ON s.manager_id = e.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY job_title ORDER BY dept_id_stddev_samp_salary DESC NULLS FIRST) dept_id_stddev_samp_salary_dense_rank,
        LAST_VALUE(salary_share, TRUE) OVER (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_salary_share,
        LEAD(dept_id_stddev_samp_salary, 1) OVER (PARTITION BY manager_id ORDER BY salary, id) next_dept_id_stddev_samp_salary,
        ROW_NUMBER() OVER w_hist row_seq
    FROM joined
    WINDOW w_hist AS (PARTITION BY dept_id ORDER BY salary_avg_5_prior, id)
),
deduped_employees AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY salary_avg_5_prior, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    dept_id,
    COUNT(*) FILTER (WHERE job_title = 'Analyst') AS analyst_rows,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT_IF(employee_job_title = 'Analyst') AS analyst_events,
    COUNT(*) AS row_count,
    SUM(dept_id_stddev_samp_salary) FILTER (WHERE job_title = 'Manager') AS manager_total_dept_id_stddev_samp_salary,
    APPROX_PERCENTILE(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM deduped_employees
GROUP BY dept_id
ORDER BY analyst_events
