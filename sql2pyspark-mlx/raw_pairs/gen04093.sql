with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        location,
        min(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) running_min_budget,
        sum(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) running_sum_budget,
        avg(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) running_avg_budget,
        max(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) running_max_budget,
        sum(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) cumulative_budget
    from departments
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from with_windows
    qualify row_number() over (partition by location order by running_min_budget desc, id) = 1
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        length(location) location_length,
        round(running_max_budget / running_avg_budget, 2) running_max_budget_per_running_avg_budget
    from deduped
    where cumulative_budget != 12009758
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        running_min_budget,
        running_max_budget,
        location_length,
        running_max_budget_per_running_avg_budget
    from filtered
    qualify row_number() over (partition by location order by running_min_budget, id) = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        last_value(running_max_budget_per_running_avg_budget) ignore nulls over (order by running_min_budget, id rows between unbounded preceding and current row) last_known_running_max_budget_per_running_avg_budget,
        first_value(location_length, true) over (order by running_max_budget, id rows between unbounded preceding and current row) first_known_location_length
    from ranked
    where running_min_budget >= 3687000
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        running_max_budget,
        location_length,
        last_known_running_max_budget_per_running_avg_budget,
        running_max_budget_per_running_avg_budget - last_known_running_max_budget_per_running_avg_budget running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget,
        running_max_budget_per_running_avg_budget - running_max_budget running_max_budget_per_running_avg_budget_minus_running_max_budget
    from scored
    where running_min_budget >= 2233000
)
-- Grouped metrics
select
    case when running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget >= 78 then 'PREMIUM' else 'STANDARD' end as running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget_tier,
    case when location_length >= 64 then 'HIGH' else 'LOW' end as location_length_tier,
    case when running_max_budget_per_running_avg_budget_minus_running_max_budget >= 63 then 'HIGH' else 'LOW' end as running_max_budget_per_running_avg_budget_minus_running_max_budget_tier,
    count(*) as row_count,
    percentile_approx(running_max_budget, array(0.25, 0.5, 0.75)) as running_max_budget_quartiles,
    min(running_max_budget) as min_running_max_budget,
    count(*) as row_count_2
from cleaned
group by grouping sets ((case when running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget >= 78 then 'PREMIUM' else 'STANDARD' end, case when location_length >= 64 then 'HIGH' else 'LOW' end, case when running_max_budget_per_running_avg_budget_minus_running_max_budget >= 63 then 'HIGH' else 'LOW' end), (case when running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget >= 78 then 'PREMIUM' else 'STANDARD' end, case when location_length >= 64 then 'HIGH' else 'LOW' end), (case when running_max_budget_per_running_avg_budget_minus_last_known_running_max_budget_per_running_avg_budget >= 78 then 'PREMIUM' else 'STANDARD' end), ());
