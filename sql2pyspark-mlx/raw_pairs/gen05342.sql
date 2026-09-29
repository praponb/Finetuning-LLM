with scored as (
    select
        id,
        location,
        ntile(5) over w_ordered as budget_bucket,
        sum(budget) over (partition by location, dept_name) as location_sum_budget,
        avg(budget) over (partition by dept_name order by budget asc, id asc rows between 2 preceding and current row) as budget_avg_2,
        round(budget / sum(budget) over (partition by dept_name, location), 4) as budget_share,
        ntile(3) over w_recent as budget_bucket_2
    from departments
    where budget < 2012000
    window w_ordered as (order by budget asc, id asc), w_recent as (partition by dept_name order by budget asc, id asc)
),
with_windows as (
    select
        *,
        lead(budget_share) over w_recent as next_budget_share,
        lead(budget_bucket) over w_recent as next_budget_bucket,
        lag(budget_bucket) over w_recent as prev_budget_bucket,
        row_number() over w_recent as event_seq
    from scored
    window w_recent as (partition by location order by budget_share asc, id asc)
),
with_windows_departments as (
    select
        *,
        round(event_seq / sum(event_seq) over (partition by location), 4) as event_seq_share,
        lag(budget_share, 1, 0) over (partition by location order by prev_budget_bucket asc, id asc) as prev_budget_share
    from with_windows
),
not_flagged as (
    select s.*
    from with_windows_departments s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.dept_id = 8
        )
),
with_windows_departments_2 as (
    select
        id,
        location,
        budget_bucket,
        location_sum_budget,
        budget_avg_2,
        budget_share,
        budget_bucket_2,
        next_budget_share,
        next_budget_bucket,
        prev_budget_bucket,
        prev_budget_share,
        avg(location_sum_budget) over w_recent as running_avg_location_sum_budget,
        count(location_sum_budget) over w_recent as running_count_location_sum_budget,
        sum(location_sum_budget) over w_recent as running_sum_location_sum_budget,
        rank() over (partition by location order by event_seq_share desc nulls last) as event_seq_share_rank
    from not_flagged
    window w_recent as (partition by location order by budget_bucket asc, id asc rows between unbounded preceding and current row)
)
select
    location,
    case when running_count_location_sum_budget >= 15 then 'LARGE' else 'SMALL' end as running_count_location_sum_budget_tier,
    count(distinct location) as distinct_locations,
    round(sum(budget_bucket_2) / nullif(count(case when location = 'Tokyo' then 1 end), 0), 2) as budget_bucket_2_per_tokyo,
    avg(prev_budget_bucket) filter (where location = 'London') as london_avg_prev_budget_bucket
from with_windows_departments_2
group by location, case when running_count_location_sum_budget >= 15 then 'LARGE' else 'SMALL' end
order by location asc
