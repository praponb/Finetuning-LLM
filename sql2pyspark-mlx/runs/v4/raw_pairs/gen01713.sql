select case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort, min(event_time) as first_event_time
from web_events
group by event_cohort
order by event_cohort desc, first_event_time
limit 1;
