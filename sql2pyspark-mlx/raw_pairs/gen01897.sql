select id, dept_name, budget, location from (select *, row_number() over (partition by dept_name order by budget, id) as pos from departments) as ranked where pos <= 1 order by dept_name asc
