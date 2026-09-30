with filtered as (
    -- Keep recent, valid rows
    select
        *,
        cast(budget as int) as budget_int,
        upper(dept_name) as dept_name_upper,
        coalesce(location, 'NONE') as location_filled
    from departments
    where location is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        budget,
        budget_int,
        dept_name_upper,
        location_filled,
        cume_dist() over (partition by location order by budget_int desc nulls first) as budget_int_cume_dist,
        last_value(location) ignore nulls over w_recent as last_known_location,
        sum(budget) over w_ordered as running_budget,
        last_value(budget, true) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as last_known_budget,
        lead(budget, 2, 0) over w_part as next_budget
    from filtered
    where location = 'London'
    window w_recent as (order by budget, id rows between unbounded preceding and current row), w_ordered as (partition by dept_name order by budget_int, id rows between unbounded preceding and current row), w_part as (partition by location order by budget, id)
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        substring(dept_name, 1, 2) as dept_name_prefix,
        coalesce(dept_name, 'NONE') as dept_name_clean
    from with_windows
    where dept_name is not null
      and budget_int_cume_dist > 0.72
),
valid_departments as (
    -- Keep recent, valid rows
    select *, last_known_budget + next_budget as last_known_budget_plus_next_budget
    from valid
    where budget_int != 51
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid_departments s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and not x.is_remote
        )
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        round(last_known_budget / sum(last_known_budget) over (partition by dept_name), 4) as last_known_budget_share,
        avg(last_known_budget_plus_next_budget) over (partition by dept_name order by budget, id rows between 5 preceding and current row) as last_known_budget_plus_next_budget_avg_5
    from kept
)
select
    id,
    dept_name,
    budget_int,
    dept_name_upper,
    location_filled,
    budget_int_cume_dist,
    running_budget,
    last_known_budget,
    dept_name_clean
from metrics;
