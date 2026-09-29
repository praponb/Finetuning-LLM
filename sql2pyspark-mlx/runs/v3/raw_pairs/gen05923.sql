select id, location, dept_name, budget, case when dept_name in ('Sales', 'Finance') then 'Group A' else 'Other' end as dept_name_group from departments where budget >= 1127000
