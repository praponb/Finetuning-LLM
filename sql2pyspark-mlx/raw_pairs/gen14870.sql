WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        ROUND(salary * 1.1, 2) adjusted_salary,
        COALESCE(job_title, 'NONE') job_title_clean
    FROM employees
    WHERE job_title IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(adjusted_salary, 1) OVER w_running prev_adjusted_salary,
        LEAD(adjusted_salary, 2) OVER (PARTITION BY manager_id ORDER BY adjusted_salary, id) next_adjusted_salary
    FROM cleaned
    WINDOW w_running AS (PARTITION BY dept_id, job_title ORDER BY adjusted_salary, id)
),
cleaned_employees AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        job_title_clean,
        prev_adjusted_salary,
        ROUND(adjusted_salary * 0.85, 2) adjusted_adjusted_salary
    FROM enriched
    WHERE job_title IS NOT NULL
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.manager_id,
        a.job_title,
        a.prev_adjusted_salary,
        a.adjusted_adjusted_salary,
        d.full_name AS employee_full_name,
        d.job_title AS employee_job_title,
        d.level
    FROM cleaned_employees a
    LEFT JOIN employees d
        ON a.manager_id <=> d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        job_title,
        adjusted_adjusted_salary,
        level,
        SUM(prev_adjusted_salary) OVER (PARTITION BY job_title ORDER BY adjusted_adjusted_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_prev_adjusted_salary,
        CUME_DIST() OVER (ORDER BY prev_adjusted_salary NULLS LAST) prev_adjusted_salary_cume_dist,
        ROW_NUMBER() OVER w_hist event_seq
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY manager_id ORDER BY adjusted_adjusted_salary, id)
)
-- Ranking and gap features
SELECT
    id,
    job_title,
    level,
    cumulative_prev_adjusted_salary,
    event_seq,
    CUME_DIST() OVER (PARTITION BY level ORDER BY adjusted_adjusted_salary DESC NULLS FIRST) adjusted_adjusted_salary_cume_dist,
    LEAD(prev_adjusted_salary_cume_dist, 2, 0) OVER (PARTITION BY level, job_title ORDER BY event_seq, id) next_prev_adjusted_salary_cume_dist
FROM metrics
WHERE adjusted_adjusted_salary >= 56;
