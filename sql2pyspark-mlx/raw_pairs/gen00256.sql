with metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over w_grp as salary_rank,
        avg(salary) over (partition by manager_id order by hire_date, id rows between 5 preceding and 1 preceding) as salary_avg_5_prior,
        sum(salary) over w_hist as cumulative_salary
    from employees
    window w_grp as (partition by job_title order by salary), w_hist as (order by hire_date, id rows between unbounded preceding and current row)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        salary_rank,
        salary_avg_5_prior
    from (
        select *, rank() over (partition by level, manager_id order by cumulative_salary desc) as dedup_rank
        from metrics
    ) x
    where dedup_rank = 1
),
with_dims as (
    -- Attach departments attributes
    select
        e.*,
        d.budget,
        d.dept_name
    from latest e
    left join departments d
        on e.dept_id = d.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary_rank,
        salary_avg_5_prior,
        budget,
        dept_name,
        trim(dept_name) as clean_dept_name
    from with_dims
    where job_title is not null
      and email like '%@startup.io'
)
-- Native SQL PIVOT
select *
from (
    select
        dept_id,
        dept_name,
        salary_rank
    from base
) src
pivot (
    sum(salary_rank) as total,
    count(1) as n
    for dept_name in ('Finance', 'Engineering')
);
