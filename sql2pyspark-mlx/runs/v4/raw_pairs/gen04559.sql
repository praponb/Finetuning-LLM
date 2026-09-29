with filtered_departments as (
    select *, case when location in ('London', 'New York') then 'Group A' else 'Other' end as location_group
    from departments
    where dept_name = 'Legal' and (dept_name in ('HR', 'Marketing') or location is null)
),
dept_name_summary as (
    select dept_name, round(stddev(budget), 2) as stddev_budget, sum(case when location = 'New York' then 1 else 0 end) as new_york_count
    from filtered_departments
    group by dept_name
)
select *
from dept_name_summary
where new_york_count < 31
order by dept_name;
