select id, manager_id, hire_date, dense_rank() over (partition by manager_id order by hire_date desc) as hire_date_dense_rank from employees;
