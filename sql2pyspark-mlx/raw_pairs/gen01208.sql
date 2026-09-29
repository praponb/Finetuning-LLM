with base as (
    select *, upper(job_title) job_title_upper
    from employees
    where hire_date >= to_date('2023-05-05')
),
metrics as (
    select
        id,
        full_name,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        job_title_upper,
        avg(salary) over (partition by job_title order by hire_date, id rows between 3 preceding and current row) last4_avg_salary,
        max(salary) over (partition by job_title order by hire_date, id rows between 3 preceding and current row) last4_max_salary,
        count(salary) over (partition by job_title order by hire_date, id rows between 3 preceding and current row) last4_count_salary
    from base
),
with_dims as (
    select
        src.*,
        d.location,
        d.budget
    from metrics src
    left join departments d
        on src.dept_id = d.id
    where d.budget > 2170000
),
joined as (
    select
        e.*,
        b.dept_name,
        b.budget as department_budget,
        b.location as department_location
    from with_dims e
    left join departments b
        on e.dept_id = b.id
)
select
    s.id,
    s.full_name,
    s.dept_id,
    s.job_title,
    s.hire_date,
    s.is_remote,
    s.job_title_upper,
    s.last4_avg_salary,
    s.location,
    case when full_name rlike '^(Henry Ford|Alice Wong)$' then 'OTHER_ZONE' else 'EXTENDED' end as full_name_zone,
    (select min(x.salary) from employees x where x.dept_id = s.dept_id) as min_employee_salary
from joined s;
