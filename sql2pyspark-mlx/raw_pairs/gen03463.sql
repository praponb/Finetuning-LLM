select * from (select case when dept_name in ('Marketing', 'Sales') then 'priority' else 'other' end as dept_name_group, location, budget from departments)
pivot (sum(budget) for location in ('New York', 'London', 'Chiang Mai', 'Singapore'))
order by dept_name_group;
