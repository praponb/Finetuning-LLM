with latest as (
    -- QUALIFY-style window filter
    select *
    from departments
    qualify row_number() over (partition by location order by budget, id) = 1
),
with_latest as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.employee_level,
        latest.salary
    from latest s
    join lateral (
            select c.level as employee_level, c.salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) latest on true
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        location,
        budget,
        employee_level,
        salary,
        dense_rank() over (partition by dept_name order by budget nulls last) budget_dense_rank,
        percent_rank() over (order by budget) budget_percent_rank
    from with_latest
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from enriched s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.job_title = 'Director'
        )
),
daily as (
    -- Multi-level aggregation
    select
        employee_level,
        count(distinct id) as distinct_ids,
        count(*) as event_count,
        stddev_samp(budget) as budget_stddev,
        size(collect_set(dept_name)) as num_dept_names
    from not_flagged
    group by employee_level
    having count(distinct id) > 1
)
select
    employee_level,
    distinct_ids,
    event_count,
    budget_stddev,
    num_dept_names
from daily
order by employee_level desc nulls first
limit 20;
