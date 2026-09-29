with department_totals as (
    select dept_id, sum(case when job_title = 'Analyst' then 1 else 0 end) analyst_count
    from employees
    where level in ('L2', 'L1')
    group by dept_id
)
select departments.dept_name, department_totals.analyst_count
from departments
left outer join department_totals on departments.id = department_totals.dept_id
where department_totals.analyst_count >= 79
order by departments.dept_name, analyst_count desc;
