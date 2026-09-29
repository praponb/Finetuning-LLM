WITH with_windows AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        SUM(salary) OVER (PARTITION BY manager_id, level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share
    FROM employees
),
filtered AS (
    SELECT *, DATE_FORMAT(hire_date, 'yyyy-MM-dd') AS hire_date_label
    FROM with_windows
    WHERE hire_date > DATE '2023-08-11'
      AND level IS NOT NULL
),
metrics AS (
    SELECT
        *,
        SUM(salary_share) OVER (ORDER BY salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_share,
        RANK() OVER (PARTITION BY job_title ORDER BY cumulative_salary NULLS LAST) AS cumulative_salary_rank,
        PERCENT_RANK() OVER (PARTITION BY manager_id ORDER BY salary DESC) AS salary_percent_rank,
        SUM(salary_share) OVER (PARTITION BY job_title, manager_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_share_2
    FROM filtered
)
SELECT
    CASE WHEN salary_percent_rank >= 0.73 THEN 'LARGE' ELSE 'SMALL' END AS salary_percent_rank_tier,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    level,
    COUNT(CASE WHEN level = 'L4' THEN 1 END) AS l4_count,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    COUNT(*) FILTER (WHERE level = 'L1') AS l1_rows,
    SUM(CASE WHEN job_title = 'Intern' THEN salary_percent_rank ELSE 0 END) AS intern_salary_percent_rank,
    APPROX_PERCENTILE(cumulative_salary_rank, ARRAY(0.25, 0.5, 0.75)) AS cumulative_salary_rank_quartiles
FROM metrics
GROUP BY GROUPING SETS ((CASE WHEN salary_percent_rank >= 0.73 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', hire_date), level), (CASE WHEN salary_percent_rank >= 0.73 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', hire_date)));
