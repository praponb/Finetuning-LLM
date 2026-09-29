SELECT YEAR(hire_date) AS hire_year, MAX(hire_date) AS last_hire_date, COUNT(CASE WHEN job_title = 'Director' THEN 1 END) AS director_count
FROM employees
GROUP BY YEAR(hire_date)
ORDER BY hire_year DESC, director_count DESC
