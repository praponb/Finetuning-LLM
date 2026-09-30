with with_last as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.budget,
        latest.employee_job_title,
        latest.level
    from departments s
    left join lateral (
            select c.job_title as employee_job_title, c.level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) latest on true
),
latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by level order by budget desc nulls last) as rank_in_group
    from with_last
    qualify rank_in_group = 1
),
with_last_departments as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.employee_hire_date,
        x.employee_salary
    from latest s
    left join lateral (
            select c.hire_date as employee_hire_date, c.salary as employee_salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_last_departments
    qualify row_number() over (partition by employee_job_title order by employee_salary desc nulls first, id) = 1
),
stats as (
    -- Grouped metrics
    select
        level,
        round(sum(budget) / nullif(count(case when employee_job_title = 'Director' then 1 end), 0), 2) as budget_per_director,
        sum(case when level = 'L5' then budget else 0 end) as l5_budget
    from ranked
    group by level
)
select level, budget_per_director
from stats
order by budget_per_director desc nulls last;
