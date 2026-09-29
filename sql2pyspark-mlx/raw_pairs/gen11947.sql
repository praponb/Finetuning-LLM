WITH employees_by_department AS (
    SELECT dept_id, ROUND((100 * SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_designer, COUNT(CASE WHEN level = 'L1' THEN 1 END) l1_count
    FROM employees
    WHERE job_title NOT IN ('Intern', 'Analyst')
    GROUP BY dept_id
)
SELECT
    d.id,
    d.dept_name,
    d.budget,
    ebd.pct_designer,
    ebd.l1_count
FROM departments d
LEFT OUTER JOIN employees_by_department ebd ON d.id = ebd.dept_id
WHERE ebd.pct_designer > 25
