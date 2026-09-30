with recent_web_events as (
    select *
    from web_events
    where page in ('/product', '/cart', '/checkout') and event_time < '2023-05-01' and event_type = 'purchase'
),
web_events_stats as (
    select case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort, count(distinct duration_sec) as unique_sessions, min(event_time) as first_event_time
    from recent_web_events
    group by case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end
)
select *
from web_events_stats
order by 1;
