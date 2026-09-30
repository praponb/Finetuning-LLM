with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(dept_name) clean_dept_name,
        coalesce(dept_name, 'DIRECT') resolved_dept_name
    from departments
    where dept_name in ('Legal', 'Finance', 'HR')
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.email not like '%@yahoo.com'
        )
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        dept_name,
        location,
        budget,
        clean_dept_name,
        resolved_dept_name,
        concat(location, '/', dept_name) location_dept_name
    from not_flagged
    where budget <= 1252000
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (partition by dept_name, location order by budget, id) budget_bucket,
        lag(budget, 2) over w_ordered prev_budget,
        first_value(budget) ignore nulls over (order by budget, id rows between unbounded preceding and current row) first_known_budget
    from valid
    window w_ordered as (order by budget, id)
)
-- Native SQL PIVOT
select *
from (
    select
        dept_name,
        location,
        prev_budget
    from metrics
) src
pivot (
    sum(prev_budget)
    for location in ('Chiang Mai' as chiang_mai, 'Tokyo' as tokyo)
)
