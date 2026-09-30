WITH matched_keys AS (
    -- Set intersection on keys
    SELECT dept_id, level
    FROM employees
    INTERSECT ALL
    SELECT dept_id, level
    FROM employees
    WHERE level != 'L1'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM employees s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.level = k.level
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    COUNT(CASE WHEN level = 'L3' THEN 1 END) AS l3_count,
    SIZE(COLLECT_SET(job_title)) AS num_job_titles,
    APPROX_PERCENTILE(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
    SUM(CASE WHEN level = 'L5' THEN salary ELSE 0 END) AS l5_salary
FROM reconciled
GROUP BY DATE_TRUNC('YEAR', hire_date)
ORDER BY year_start DESC
