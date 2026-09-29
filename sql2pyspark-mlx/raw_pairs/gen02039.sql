select id, dept_name, budget from (select *, rank() over (partition by location order by budget desc nulls last) as rank_in_group from departments) x where rank_in_group = 1;
