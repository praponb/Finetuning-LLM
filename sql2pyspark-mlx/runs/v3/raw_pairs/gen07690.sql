SELECT id, salary, job_title, email FROM employees WHERE salary >= (SELECT AVG(salary) FROM employees) ORDER BY job_title DESC;
