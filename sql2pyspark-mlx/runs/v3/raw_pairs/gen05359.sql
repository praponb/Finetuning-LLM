select case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort, count(duration_sec) as duration_sec_count
from web_events
where session_id in (4, 21)
group by 1
having duration_sec_count > 4
order by duration_sec_count desc;
