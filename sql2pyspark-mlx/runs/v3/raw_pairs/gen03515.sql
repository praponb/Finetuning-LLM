SELECT hire_date, job_title FROM employees WHERE job_title = 'Analyst' EXCEPT SELECT hire_date, job_title FROM employees WHERE job_title = 'Engineer';
