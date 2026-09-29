with joined as (
    -- Attach employees attributes
    select src.*, b.salary as employee_salary
    from employees src
    left join employees b
        on src.manager_id = b.id
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        employee_salary,
        last_value(job_title, true) over (order by salary, id rows between unbounded preceding and current row) as last_known_job_title,
        avg(salary) over (order by salary, id rows between 3 preceding and current row) as salary_avg_3,
        avg(employee_salary) over (partition by job_title, dept_id order by hire_date, id rows between 6 preceding and 1 preceding) as employee_salary_avg_6_prior,
        last_value(job_title) ignore nulls over (partition by job_title, dept_id order by hire_date, id rows between unbounded preceding and current row) as last_known_job_title_2,
        row_number() over (partition by dept_id, job_title order by hire_date desc, id) as seq_no
    from joined
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        upper(trim(email)) as email_normalized,
        round(salary_avg_3 / employee_salary, 2) as salary_avg_3_per_employee_salary
    from scored
    where hire_date > to_date('2023-08-27')
      and level is not null
      and seq_no is null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from cleaned
    qualify row_number() over (partition by dept_id order by employee_salary_avg_6_prior desc nulls last, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        employee_salary,
        last_known_job_title,
        salary_avg_3,
        employee_salary_avg_6_prior,
        last_known_job_title_2,
        seq_no,
        email_normalized,
        salary_avg_3_per_employee_salary,
        lead(salary, 2) over w_running as next_salary,
        avg(salary) over w_ordered as salary_avg_3_prior,
        avg(salary_avg_3) over (order by seq_no, id rows between 5 preceding and current row) as salary_avg_3_avg_5
    from latest
    window w_running as (partition by dept_id order by employee_salary, id), w_ordered as (order by hire_date, id rows between 3 preceding and 1 preceding)
),
cleaned_employees as (
    -- Keep recent, valid rows
    select *, concat(job_title, ' ', level) as job_title_level
    from metrics
    where hire_date >= to_date('2023-11-04')
      and job_title is not null
      and employee_salary <= 101000
),
valid as (
    -- Basic cleansing and defaults
    select *, coalesce(salary_avg_3, 0) as salary_avg_3_filled
    from cleaned_employees
    where next_salary > 122000
)
-- Ranking and gap features
select
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    employee_salary,
    last_known_job_title,
    salary_avg_3,
    employee_salary_avg_6_prior,
    last_known_job_title_2,
    seq_no,
    email_normalized,
    salary_avg_3_per_employee_salary,
    next_salary,
    salary_avg_3_prior,
    salary_avg_3_avg_5,
    job_title_level,
    salary_avg_3_filled,
    first_value(employee_salary_avg_6_prior, true) over w_ordered as first_known_employee_salary_avg_6_prior,
    lag(salary_avg_3_filled, 1) over (partition by dept_id order by salary, id) as prev_salary_avg_3_filled,
    lag(salary_avg_3_per_employee_salary, 2) over (partition by job_title order by salary_avg_3_filled, id) as prev_salary_avg_3_per_employee_salary,
    ntile(4) over (partition by job_title order by seq_no, id) as seq_no_bucket,
    percent_rank() over w_ordered_2 as salary_percent_rank
from valid
window w_ordered as (order by salary_avg_3_filled, id rows between unbounded preceding and current row), w_ordered_2 as (order by salary desc);
