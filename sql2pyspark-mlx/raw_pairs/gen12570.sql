with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name,
        budget,
        avg(budget) over (partition by dept_name) as dept_name_avg_budget,
        row_number() over (order by budget desc, id asc) as event_seq
    from departments
),
enriched_departments as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        event_seq,
        lag(dept_name_avg_budget) over w_hist as prev_dept_name_avg_budget,
        lag(budget) over w_hist as prev_budget,
        row_number() over w_hist as event_seq_2,
        row_number() over (partition by dept_name order by dept_name_avg_budget asc, id asc) as event_seq_3
    from enriched
    window w_hist as (partition by dept_name order by budget asc, id asc)
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from enriched_departments
    intersect
    select id
    from departments
    where location in ('Singapore', 'Tokyo')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_departments s
    inner join remaining_keys k on s.id = k.id
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        event_seq,
        prev_dept_name_avg_budget,
        prev_budget,
        event_seq_2,
        event_seq_3,
        round(prev_budget * 0.85, 2) as prev_budget_adjusted,
        case when dept_name = 'HR' then 1 else 0 end as is_hr
    from kept_rows
    where dept_name is distinct from 'Marketing'
)
-- Aggregate with conditional metrics
select
    case when prev_dept_name_avg_budget >= 4502613 then 'HIGH' else 'LOW' end as prev_dept_name_avg_budget_tier,
    case when is_hr >= 81 then 'HIGH' else 'LOW' end as is_hr_tier,
    percentile_approx(is_hr, array(0.25, 0.5, 0.75)) as is_hr_quartiles,
    percentile_approx(event_seq, 0.9) as event_seq_p90,
    avg(prev_budget_adjusted) as avg_prev_budget_adjusted,
    round(avg(is_hr), 2) as avg_is_hr,
    round(avg(prev_dept_name_avg_budget), 2) as avg_prev_dept_name_avg_budget,
    grouping_id() as grouping_level
from valid
group by rollup (case when prev_dept_name_avg_budget >= 4502613 then 'HIGH' else 'LOW' end, case when is_hr >= 81 then 'HIGH' else 'LOW' end)
order by event_seq_p90 desc;
