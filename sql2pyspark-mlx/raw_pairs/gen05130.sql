WITH metrics AS (
    SELECT
        id,
        full_name,
        manager_id,
        hire_date,
        is_remote,
        level,
        SUM(salary) OVER (PARTITION BY manager_id, job_title ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS seq_no
    FROM employees
)
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    PERCENTILE_APPROX(seq_no, 0.95) AS seq_no_p95,
    ROUND(SUM(running_salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS running_salary_per_l1,
    SUM(CASE WHEN level = 'L3' THEN seq_no ELSE 0 END) AS l3_seq_no,
    APPROX_PERCENTILE(running_salary, ARRAY(0.25, 0.5, 0.75)) AS running_salary_quartiles,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY DATE_TRUNC('YEAR', hire_date)
HAVING PERCENTILE_APPROX(seq_no, 0.95) >= 0
