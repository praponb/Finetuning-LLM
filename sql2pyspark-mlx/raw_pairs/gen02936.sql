with valid as (
    -- Basic cleansing and defaults
    select
        *,
        substring(location, 1, 4) as location_prefix,
        case when location = 'Berlin' then 1 else 0 end as is_berlin
    from departments
    where dept_name is not null
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.dept_id in (10, 19)
        )
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.dept_name,
        s.is_berlin,
        x.salary
    from eligible s
    join lateral (
            select c.salary
            from employees c
            where c.dept_id = s.id
            order by c.hire_date desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.salary < 62000
        )
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.dept_id in (22, 38, 40)
        )
)
-- Grouped metrics
select
    dept_name,
    round(avg(is_berlin), 2) as avg_is_berlin,
    sort_array(collect_set(dept_name)) as dept_name_list,
    count(case when dept_name = 'Marketing' then 1 end) as marketing_count,
    count(*) filter (where dept_name = 'Sales') as sales_rows
from kept
group by dept_name
order by sales_rows desc
