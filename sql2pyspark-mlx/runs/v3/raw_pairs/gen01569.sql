select id, dept_id, salary, avg(salary) over (partition by dept_id) dept_id_avg_salary, rank() over (partition by dept_id order by salary desc) salary_rank from employees;
