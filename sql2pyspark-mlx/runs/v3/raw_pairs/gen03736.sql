select manager_id, hire_date, email, round(salary * 1.07, 2) as salary_scaled from employees where level = 'L2'
