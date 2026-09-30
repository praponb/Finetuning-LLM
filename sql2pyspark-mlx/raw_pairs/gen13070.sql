SELECT level, job_title FROM employees EXCEPT SELECT level, job_title FROM employees WHERE job_title != 'Intern';
