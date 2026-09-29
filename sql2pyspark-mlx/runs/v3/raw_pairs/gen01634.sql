with filtered_employees as (
    select *
    from employees
    where is_remote
),
summary as (
    select dept_id, concat_ws(', ', sort_array(collect_set(job_title))) as job_title_values, min(salary) as min_salary
    from filtered_employees
    group by dept_id
)
select dept_id, job_title_values, min_salary
from summary
order by 2, dept_id
limit 10
