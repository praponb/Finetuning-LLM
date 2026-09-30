with department_summary as (
    select dept_id, sum(case when job_title = 'Designer' then salary else 0 end) as designer_salary, concat_ws(', ', sort_array(collect_set(job_title))) as job_title_values
    from employees
    group by dept_id
)
select
    departments.id,
    departments.budget,
    departments.dept_name,
    department_summary.designer_salary,
    department_summary.job_title_values
from departments
left join department_summary on departments.id = department_summary.dept_id;
