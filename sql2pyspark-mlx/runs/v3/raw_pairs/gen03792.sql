select id, dept_id, salary, percent_rank() over (partition by dept_id order by salary) as salary_pct_rank from employees order by dept_id desc, salary_pct_rank desc
