with valid_web_events as (
    select *
    from web_events
    where page in ('/checkout', '/home', '/product')
),
event_cohort_summary as (
    select
        case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort,
        case when event_type in ('click', 'search') then 'group_a' else 'other' end as event_type_group,
        sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_count,
        count(distinct duration_sec) as unique_pages
    from valid_web_events
    group by case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end, case when event_type in ('click', 'search') then 'group_a' else 'other' end
)
select
    event_cohort,
    event_type_group,
    purchase_count,
    unique_pages
from event_cohort_summary
where purchase_count < 47
order by event_cohort, event_type_group
limit 10
