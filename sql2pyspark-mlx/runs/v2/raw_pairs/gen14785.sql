select manager_id, max(hire_date) latest_hire_date from employees where level not in ('L2', 'L4', 'L1') and salary < 73000 group by manager_id order by manager_id desc;
