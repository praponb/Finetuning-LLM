select id, manager_id, hire_date, full_name from (select id, manager_id, hire_date, full_name, rank() over (partition by manager_id order by hire_date) as rn from employees) as ranked where rn = 1;
