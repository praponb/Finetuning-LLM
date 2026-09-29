SELECT
    id,
    salary,
    FLOOR(salary) AS salary_whole,
    LOWER(job_title) AS job_title_lower
FROM employees
WHERE dept_id IN (15, 30);
