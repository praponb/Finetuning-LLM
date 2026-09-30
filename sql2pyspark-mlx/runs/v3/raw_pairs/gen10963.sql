select case when dept_name in ('Marketing', 'Finance') then 'priority' else 'other' end as dept_name_group, concat_ws(', ', sort_array(collect_set(dept_name))) as dept_name_values
from departments
group by dept_name_group
order by dept_name_values;
