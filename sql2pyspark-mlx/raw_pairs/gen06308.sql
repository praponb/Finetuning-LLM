with deduped as (
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level
    from employees
    qualify row_number() over (partition by job_title order by salary, id) = 1
),
latest as (
    select *, row_number() over (partition by dept_id order by hire_date desc nulls last, id) as rank_in_group
    from deduped
    qualify rank_in_group = 1
),
with_dims as (
    select
        src.id,
        src.email,
        src.manager_id,
        src.salary,
        src.hire_date,
        src.is_remote,
        src.level,
        src.rank_in_group,
        p.full_name,
        p.salary as employee_salary
    from latest src
    inner join employees p
        on src.manager_id = p.id
),
cleaned as (
    select
        id,
        email,
        manager_id,
        salary,
        hire_date,
        level,
        rank_in_group,
        employee_salary,
        substring(level, 1, 4) as level_prefix
    from with_dims
    where hire_date > to_date('2023-05-19')
      and manager_id in (10, 18, 20, 35)
),
with_dims_employees as (
    select
        e.*,
        d.job_title,
        d.salary as employee_salary_2,
        d.hire_date as employee_hire_date
    from cleaned e
    left join employees d
        on e.manager_id = d.id
)
select *
from (
    select
        id,
        manager_id,
        salary,
        employee_salary_2,
        employee_salary
    from with_dims_employees
) src
unpivot (
    metric_value for metric in (salary as SALARY_VAL, employee_salary_2, employee_salary)
);
