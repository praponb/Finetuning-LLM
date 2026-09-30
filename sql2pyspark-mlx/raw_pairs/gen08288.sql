select dept_id, level, month(hire_date) as hire_date_month from employees where full_name like '%Jac%' order by hire_date_month
