SELECT d.id, d.budget, t.director_count
FROM departments d
INNER JOIN (SELECT dept_id, COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id
ORDER BY id;
