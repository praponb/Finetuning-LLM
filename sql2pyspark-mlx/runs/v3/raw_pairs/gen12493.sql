select id, dept_id, hire_date, row_number() over (partition by dept_id order by hire_date desc, id) as rn from employees;
