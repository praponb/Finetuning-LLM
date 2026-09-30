with filtered as (
    select
        *,
        round(budget * 100, 2) as adjusted_budget,
        concat(dept_name, '/', location) as dept_name_location,
        coalesce(location, 'N/A') as location_filled
    from departments
    where location is not null
      and location is distinct from 'Bangkok'
),
valid as (
    select
        *,
        budget - adjusted_budget as budget_minus_adjusted_budget,
        concat_ws('-', dept_name, location) as dept_name_location_key
    from filtered
    where budget > 3108000
),
matched_keys as (
    select id
    from valid
    except all
    select id
    from departments
    where dept_name not in ('HR', 'Finance', 'Sales')
),
verified as (
    select s.*
    from valid s
    inner join matched_keys k on s.id = k.id
),
not_flagged as (
    select s.*
    from verified s
    where exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.email like '%@company.co'
        )
),
enriched as (
    select
        *,
        max(budget) over (partition by location, dept_name) as location_max_budget,
        percent_rank() over (partition by location order by adjusted_budget nulls first) as adjusted_budget_percent_rank,
        sum(budget_minus_adjusted_budget) over (partition by location) as location_sum_budget_minus_adjusted_budget
    from not_flagged
    where adjusted_budget between 52 and 75
),
base as (
    select
        id,
        dept_name,
        location,
        budget,
        adjusted_budget,
        dept_name_location,
        budget_minus_adjusted_budget,
        location_max_budget,
        adjusted_budget_percent_rank,
        location_sum_budget_minus_adjusted_budget,
        budget + adjusted_budget_percent_rank as budget_plus_adjusted_budget_percent_rank,
        coalesce(dept_name, 'DIRECT') as resolved_dept_name
    from enriched
    where adjusted_budget <= 58
),
not_flagged_departments as (
    select s.*
    from base s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.job_title is null
        )
),
stats as (
    select
        dept_name,
        case when budget_minus_adjusted_budget >= 82 then 'PREMIUM' else 'STANDARD' end as budget_minus_adjusted_budget_tier,
        location,
        sort_array(collect_set(dept_name)) as dept_name_list,
        count_if(dept_name = 'HR') as hr_events,
        approx_percentile(budget_plus_adjusted_budget_percent_rank, array(0.25, 0.5, 0.75)) as budget_plus_adjusted_budget_percent_rank_quartiles,
        grouping_id() as grouping_id
    from not_flagged_departments
    group by grouping sets ((dept_name, case when budget_minus_adjusted_budget >= 82 then 'PREMIUM' else 'STANDARD' end, location), (dept_name, case when budget_minus_adjusted_budget >= 82 then 'PREMIUM' else 'STANDARD' end))
    having count_if(dept_name = 'HR') >= 1
)
select
    budget_minus_adjusted_budget_tier,
    location,
    hr_events,
    budget_plus_adjusted_budget_percent_rank_quartiles,
    grouping_id
from stats
order by grouping_id desc nulls first
