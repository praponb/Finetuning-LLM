with ranked as (
    select *
    from employees
    qualify row_number() over (partition by manager_id order by salary desc, id) = 1
),
with_dims as (
    select e.*, d.location
    from ranked e
    left join departments d
        on e.dept_id = d.id
),
enriched as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        location,
        lag(salary, 2, 0) over (partition by job_title order by hire_date, id) as prev_salary,
        ntile(5) over (partition by manager_id order by salary desc, id) as salary_bucket
    from with_dims
),
with_dims_employees as (
    select src.*, p.location as department_location
    from enriched src
    inner join departments p
        on src.dept_id = p.id
),
with_dims_employees_2 as (
    select
        e.id,
        e.dept_id,
        e.manager_id,
        e.salary,
        e.location,
        e.salary_bucket,
        p.salary as employee_salary,
        p.job_title as employee_job_title,
        p.email
    from with_dims_employees e
    inner join employees p
        on e.manager_id = p.id
    where p.dept_id in (19, 39)
)
select
    id,
    dept_id,
    salary,
    location,
    employee_salary,
    employee_job_title,
    case when location rlike '^(Bangkok|Berlin)$' then 'CORE' when location rlike '^(Chiang Mai|New York)$' then 'PRIORITY' else 'GROUP_B' end as location_zone
from with_dims_employees_2
order by location_zone desc nulls first;
