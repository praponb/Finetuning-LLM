select id, dept_id, hire_date from (select *, row_number() over (partition by dept_id order by hire_date desc, id) as rn from employees where salary between 61000 and 115000) as ranked where rn <= 2;
