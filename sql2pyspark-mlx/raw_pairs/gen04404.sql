select dept_id, hire_date, full_name, date_format(hire_date, 'yyyy-MM') as hire_date_label from employees where manager_id = 11 order by full_name, hire_date_label;
