select id, level, salary, manager_id, avg(salary) over (partition by level) level_avg_salary, percent_rank() over (partition by manager_id order by salary desc) salary_pct_rank from employees
