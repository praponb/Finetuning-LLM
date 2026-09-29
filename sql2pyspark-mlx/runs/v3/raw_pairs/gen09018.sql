with base as (
    select *
    from departments
    where location = 'New York' and budget <= 3686000 and dept_name = 'Engineering'
),
summary as (
    select case when dept_name in ('Marketing', 'Engineering') then 'core' else 'other' end dept_name_group, sum(case when dept_name = 'Engineering' then 1 else 0 end) engineering_count, round(stddev(budget), 2) stddev_budget
    from base
    group by case when dept_name in ('Marketing', 'Engineering') then 'core' else 'other' end
)
select dept_name_group, engineering_count, stddev_budget
from summary
order by engineering_count desc, dept_name_group
limit 1
