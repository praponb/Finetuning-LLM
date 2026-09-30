with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        trim(location) as clean_location,
        coalesce(location, 'UNKNOWN') as resolved_location
    from departments
    where location is not null
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect
    select id
    from departments
    where budget between 2259000 and 3224000
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join remaining_keys k on s.id = k.id
),
metrics as (
    -- Ranking and gap features
    select
        dept_name,
        budget,
        clean_location,
        resolved_location,
        round(budget / sum(budget) over (partition by location), 4) as budget_share,
        cume_dist() over w_ordered as budget_cume_dist,
        stddev_samp(budget) over (partition by location) as location_stddev_samp_budget
    from verified
    window w_ordered as (partition by dept_name order by budget desc nulls last)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        sum(location_stddev_samp_budget) over w_recent as dept_name_sum_location_stddev_samp_budget,
        rank() over w_running as budget_rank,
        dense_rank() over (partition by dept_name order by location_stddev_samp_budget desc nulls first) as location_stddev_samp_budget_dense_rank
    from metrics
    where budget < 2778000
    window w_recent as (partition by dept_name), w_running as (partition by dept_name order by budget desc nulls last)
),
filtered as (
    -- Keep recent, valid rows
    select
        dept_name,
        budget,
        clean_location,
        budget_share,
        budget_cume_dist,
        location_stddev_samp_budget,
        budget_rank,
        case when dept_name_sum_location_stddev_samp_budget >= 18894154 then 'High' when dept_name_sum_location_stddev_samp_budget >= 9809408 then 'Medium' else 'Low' end as dept_name_sum_location_stddev_samp_budget_tier,
        case when budget_rank >= 7 then 'High' when budget_rank >= 3 then 'Medium' else 'Low' end as budget_rank_band
    from scored
    where dept_name is not null
      and budget >= 1443000
)
-- Aggregate with conditional metrics
select
    dept_name,
    count(*) as row_count,
    sort_array(collect_set(dept_name)) as dept_name_list
from filtered
group by dept_name
order by row_count desc
