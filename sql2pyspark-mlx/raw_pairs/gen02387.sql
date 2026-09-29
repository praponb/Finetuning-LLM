with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when salary >= 143000 then 'High' when salary >= 65000 then 'Medium' else 'Low' end as salary_band,
        ceil(salary) as salary_whole
    from employees
    where level is distinct from 'L3'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        min(salary) over (partition by job_title, manager_id order by salary_whole asc, id asc rows between 2 preceding and current row) as last3_min_salary,
        avg(salary) over (partition by job_title, manager_id order by salary_whole asc, id asc rows between 2 preceding and current row) as last3_avg_salary,
        count(salary) over (partition by job_title, manager_id order by salary_whole asc, id asc rows between 2 preceding and current row) as last3_count_salary,
        count(salary_whole) over (partition by level) as level_count_salary_whole
    from base
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        salary_whole,
        last3_min_salary,
        last3_avg_salary,
        level_count_salary_whole,
        floor(last3_min_salary) as last3_min_salary_whole,
        concat(full_name, ' ', email) as full_name_email,
        coalesce(job_title, 'N/A') as job_title_clean
    from scored
    where hire_date >= to_date('2023-06-26')
      and job_title is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by dept_id, job_title order by salary asc, id asc) as latest_rank
        from valid
    ) t
    where latest_rank = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(level_count_salary_whole) over (partition by year(hire_date), manager_id order by level_count_salary_whole asc, id asc rows between 6 preceding and 1 preceding) as level_count_salary_whole_avg_6_prior,
        avg(last3_avg_salary) over (partition by manager_id order by last3_min_salary_whole asc, id asc rows between 6 preceding and current row) as last3_avg_salary_avg_6,
        last_value(level_count_salary_whole) ignore nulls over (partition by manager_id order by latest_rank asc, id asc rows between unbounded preceding and current row) as last_known_level_count_salary_whole
    from deduped
    where job_title is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        salary_whole,
        last3_min_salary,
        last3_min_salary_whole,
        full_name_email,
        level_count_salary_whole_avg_6_prior,
        sum(salary) over (partition by dept_id, manager_id) as dept_id_sum_salary,
        avg(level_count_salary_whole) over (partition by dept_id order by hire_date asc, id asc rows between 3 preceding and 1 preceding) as level_count_salary_whole_avg_3_prior
    from with_windows
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            dept_id,
            job_title,
            last3_min_salary
        from metrics
    ) src
    pivot (
        sum(last3_min_salary) as amount,
        count(1) as item_count
        for job_title in ('Engineer' as engineer, 'Director' as director, 'Analyst' as analyst)
    )
)
select
    s.dept_id,
    s.engineer_item_count,
    s.director_amount,
    s.director_item_count,
    s.analyst_amount,
    s.analyst_item_count,
    (select min(t2.salary) from employees t2 where t2.dept_id = s.dept_id) as min_employee_salary
from by_type s
order by analyst_amount desc
