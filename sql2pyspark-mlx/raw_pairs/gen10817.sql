with scored as (
    -- Window metrics over several different partitions
    select
        *,
        lag(salary) over w_ordered as prev_salary,
        lead(salary) over w_ordered as next_salary,
        row_number() over w_ordered as row_seq,
        ntile(3) over (partition by dept_id, job_title order by salary asc, id asc) as salary_bucket
    from employees
    window w_ordered as (partition by manager_id, dept_id order by hire_date asc, id asc)
)
select
    id,
    full_name,
    email,
    job_title,
    salary,
    is_remote,
    next_salary,
    row_seq
from scored
order by id desc nulls last;
