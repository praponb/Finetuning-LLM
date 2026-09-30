select id, dept_name, budget from (select *, dense_rank() over (partition by dept_name order by budget desc) as pos from departments where budget <= 2398000) as r where pos <= 3
