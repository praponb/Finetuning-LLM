select id, location, budget, row_number() over (partition by location order by budget, id) row_num from departments where location is not null
