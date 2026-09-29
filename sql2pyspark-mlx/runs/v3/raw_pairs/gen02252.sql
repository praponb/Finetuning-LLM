select id, dept_name, budget, row_number() over (partition by dept_name order by budget, id) as rn from departments order by id desc, rn asc;
