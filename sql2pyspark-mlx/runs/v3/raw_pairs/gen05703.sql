SELECT email FROM employees WHERE job_title IN ('Engineer', 'Manager') EXCEPT SELECT email FROM employees WHERE salary <= 104000 ORDER BY email;
