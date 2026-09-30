select id, location, dept_name, case when dept_name in ('Marketing', 'Finance') then 'Core' else 'Other' end as dept_name_group from departments order by dept_name_group, id limit 1;
