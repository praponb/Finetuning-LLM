with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(dept_name) as dept_name_upper,
        coalesce(dept_name, 'DIRECT') as dept_name_clean
    from departments
    where location is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.salary > 62000
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by dept_name order by budget desc, id) as dedup_rank
        from eligible
    ) ranked
    where dedup_rank = 1
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.employee_hire_date,
        x.employee_salary
    from deduped s
    left join lateral (
            select c.hire_date as employee_hire_date, c.salary as employee_salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) x on true
),
eligible_departments as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.hire_date >= s.employee_hire_date
        )
)
select
    id,
    dept_name,
    budget,
    dept_name_upper,
    dept_name_clean,
    dedup_rank,
    employee_hire_date
from eligible_departments;
