with department_totals as (
    select dept_id, max(hire_date) as last_hire_date, sum(case when level = 'L1' then 1 else 0 end) as l1_count
    from employees
    where salary <= 58000
    group by dept_id
)
select
    d.id,
    d.dept_name,
    d.location,
    dt.last_hire_date,
    dt.l1_count
from departments d
inner join department_totals dt on d.id = dt.dept_id;
