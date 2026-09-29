WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(salary) OVER w_part AS prev_salary,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM employees
    WHERE level IN ('L5', 'L3')
    WINDOW w_part AS (PARTITION BY level ORDER BY salary, id)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    CASE WHEN salary >= 127000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    COUNT(DISTINCT dept_id) AS distinct_dept_ids,
    ROUND(SUM(prev_salary) / NULLIF(COUNT(CASE WHEN job_title = 'Manager' THEN 1 END), 0), 2) AS prev_salary_per_manager,
    SUM(salary) FILTER (WHERE level = 'L1') AS l1_total_salary,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    SUM(prev_salary) FILTER (WHERE level = 'L5') AS l5_total_prev_salary
FROM enriched
GROUP BY CUBE (DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('QUARTER', hire_date), CASE WHEN salary >= 127000 THEN 'LARGE' ELSE 'SMALL' END)
HAVING SUM(prev_salary) FILTER (WHERE level = 'L5') > 0;
