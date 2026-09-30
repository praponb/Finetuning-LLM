select id, manager_id, salary, percent_rank() over (partition by manager_id order by salary desc) as salary_pct_rank from employees where is_remote = false;
