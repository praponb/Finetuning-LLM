SELECT QUARTER(hire_date) AS hire_quarter, MIN(salary) AS min_salary, SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list FROM employees GROUP BY QUARTER(hire_date) HAVING min_salary > 63000
