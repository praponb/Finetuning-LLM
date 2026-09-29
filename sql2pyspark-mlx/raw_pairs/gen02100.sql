WITH base AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        is_remote,
        level,
        LOWER(level) level_lower,
        COALESCE(job_title, 'DIRECT') job_title_filled
    FROM employees
    WHERE hire_date > DATE '2023-05-31'
),
filtered AS (
    SELECT *, CAST(is_remote AS INT) is_remote_flag
    FROM base
    WHERE NOT is_remote
),
metrics AS (
    SELECT
        *,
        LEAD(is_remote_flag) OVER w_ordered next_is_remote_flag,
        LAG(is_remote_flag) OVER w_ordered prev_is_remote_flag,
        ROW_NUMBER() OVER w_ordered event_seq,
        SUM(is_remote_flag) OVER (PARTITION BY manager_id ORDER BY is_remote_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_is_remote_flag
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY manager_id, job_title ORDER BY is_remote_flag, id)
),
joined AS (
    SELECT
        a.*,
        e.job_title AS employee_job_title,
        e.salary
    FROM metrics a
    LEFT JOIN employees e
        ON a.manager_id = e.id
    WHERE e.email LIKE '%@outlook.com'
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running seq_no,
        FIRST_VALUE(next_is_remote_flag, TRUE) OVER (PARTITION BY job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_next_is_remote_flag,
        LEAD(running_is_remote_flag, 1) OVER w_grp next_running_is_remote_flag,
        LAG(event_seq, 1) OVER w_hist prev_event_seq
    FROM joined
    WINDOW w_running AS (PARTITION BY job_title, employee_job_title ORDER BY next_is_remote_flag DESC, id), w_grp AS (PARTITION BY job_title ORDER BY salary, id), w_hist AS (PARTITION BY employee_job_title ORDER BY next_is_remote_flag, id)
)
SELECT
    employee_job_title,
    level,
    CASE WHEN first_known_next_is_remote_flag >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_next_is_remote_flag_tier,
    ROUND(AVG(next_is_remote_flag), 2) AS avg_next_is_remote_flag,
    COUNT(CASE WHEN employee_job_title = 'Designer' THEN 1 END) AS designer_count,
    SUM(CASE WHEN level = 'L3' THEN event_seq ELSE 0 END) AS l3_event_seq,
    MAX(salary) AS max_salary,
    PERCENTILE_APPROX(first_known_next_is_remote_flag, 0.5) AS first_known_next_is_remote_flag_p50,
    SUM(CASE WHEN employee_job_title = 'Designer' THEN is_remote_flag ELSE 0 END) AS designer_is_remote_flag
FROM with_windows
GROUP BY GROUPING SETS ((employee_job_title, level, CASE WHEN first_known_next_is_remote_flag >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END), (employee_job_title, level), (employee_job_title))
ORDER BY designer_is_remote_flag NULLS FIRST;
