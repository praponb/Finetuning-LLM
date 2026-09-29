with recent_departments as (
    select *, case when location = 'London' then 1 else 0 end is_london
    from departments
    where location in ('New York', 'Bangkok', 'London')
),
summary as (
    select case when dept_name in ('Finance', 'HR') then 'group_a' else 'other' end dept_name_group, concat_ws(', ', sort_array(collect_set(dept_name))) dept_name_values
    from recent_departments
    group by case when dept_name in ('Finance', 'HR') then 'group_a' else 'other' end
)
select dept_name_group, dept_name_values
from summary
order by dept_name_group asc;
