select * from (select id, location, budget, dept_name, row_number() over (partition by location order by budget desc, id) as rnk from departments where budget < 2720000) as t where rnk = 1;
