with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        lower(location) as location_lower,
        case when dept_name in ('Legal', 'Sales') then 'Priority' else 'Other' end as dept_name_group
    from departments
    where location is not null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        count(budget) over (partition by dept_name) as dept_name_count_budget,
        row_number() over (order by budget asc, id asc) as row_seq,
        sum(budget) over (partition by location order by budget asc, id asc rows between unbounded preceding and current row) as cumulative_budget
    from filtered
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        dept_name,
        location,
        dept_name_group,
        cumulative_budget,
        ntile(3) over (partition by dept_name order by cumulative_budget desc, id asc) as cumulative_budget_bucket,
        sum(cumulative_budget) over (order by cumulative_budget asc, id asc rows between unbounded preceding and current row) as running_cumulative_budget
    from scored
),
filtered_departments as (
    -- Keep recent, valid rows
    select *, ceil(running_cumulative_budget) as running_cumulative_budget_whole
    from with_windows
    where running_cumulative_budget != 39571389
),
grouped as (
    -- Multi-level aggregation
    select
        case when running_cumulative_budget >= 95195832 then 'HIGH' else 'LOW' end as running_cumulative_budget_tier,
        case when cumulative_budget_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as cumulative_budget_bucket_tier,
        approx_percentile(running_cumulative_budget_whole, 0.5) as running_cumulative_budget_whole_p50,
        sum(case when dept_name = 'Sales' then cumulative_budget_bucket else 0 end) as sales_cumulative_budget_bucket,
        size(collect_set(dept_name)) as num_dept_names
    from filtered_departments
    group by grouping sets ((case when running_cumulative_budget >= 95195832 then 'HIGH' else 'LOW' end, case when cumulative_budget_bucket >= 8 then 'PREMIUM' else 'STANDARD' end), (case when running_cumulative_budget >= 95195832 then 'HIGH' else 'LOW' end), ())
    having size(collect_set(dept_name)) > 1
)
select
    running_cumulative_budget_tier,
    cumulative_budget_bucket_tier,
    running_cumulative_budget_whole_p50,
    sales_cumulative_budget_bucket
from grouped
