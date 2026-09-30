with base as (
    select
        *,
        round(budget * 0.85, 2) as adjusted_budget,
        coalesce(dept_name, 'UNKNOWN') as dept_name_filled
    from departments
    where dept_name is not null
      and budget <= 4010000
),
metrics as (
    select
        *,
        dense_rank() over w_running as budget_dense_rank,
        sum(adjusted_budget) over w_ordered as cumulative_adjusted_budget,
        first_value(budget, true) over (partition by dept_name order by adjusted_budget asc, id asc rows between unbounded preceding and current row) as first_known_budget,
        ntile(3) over (order by adjusted_budget asc, id asc) as adjusted_budget_bucket,
        sum(adjusted_budget) over w_part as cumulative_adjusted_budget_2
    from base
    window w_running as (partition by location order by budget asc nulls first), w_ordered as (partition by location order by adjusted_budget asc, id asc rows between unbounded preceding and current row), w_part as (partition by location order by budget asc, id asc rows between unbounded preceding and current row)
),
eligible as (
    select s.*
    from metrics s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.hire_date >= '2024-11-10'
        )
),
metrics_departments as (
    select
        id,
        dept_name,
        location,
        budget,
        adjusted_budget,
        dept_name_filled,
        budget_dense_rank,
        cumulative_adjusted_budget,
        first_known_budget,
        adjusted_budget_bucket,
        cumulative_adjusted_budget_2,
        last_value(location) ignore nulls over (partition by dept_name order by adjusted_budget asc, id asc rows between unbounded preceding and current row) as last_known_location,
        ntile(4) over w_hist as first_known_budget_bucket,
        count(cumulative_adjusted_budget) over w_ordered as location_count_cumulative_adjusted_budget
    from eligible
    window w_hist as (partition by dept_name order by first_known_budget asc, id asc), w_ordered as (partition by location, dept_name)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by location order by first_known_budget desc, id asc) as dedup_rank
        from metrics_departments
    ) x
    where dedup_rank = 1
)
select
    id,
    dept_name,
    budget,
    adjusted_budget,
    dept_name_filled,
    budget_dense_rank,
    first_known_budget,
    adjusted_budget_bucket,
    cumulative_adjusted_budget_2,
    case when dept_name rlike '^(Marketing|HR)$' then 'GROUP_A' else 'OTHER_ZONE' end as dept_name_zone
from ranked
order by id asc
limit 5
