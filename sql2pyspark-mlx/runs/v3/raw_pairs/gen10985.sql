SELECT emp.email employee_email, emp.job_title employee_job_title, mgr.dept_id manager_dept_id
FROM employees AS emp
JOIN employees AS mgr ON emp.manager_id = mgr.id
WHERE emp.manager_id IN (13, 21, 29, 34);
