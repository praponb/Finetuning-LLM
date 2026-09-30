select case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end as event_cohort, max(event_time) as latest_event_time, count(case when page = '/cart' then 1 end) as cart_count
from web_events
where hour(event_time) between 10 and 18
group by case when year(event_time) < 2024 then 'before_2024' when year(event_time) = 2024 then '2024' else 'after_2024' end
having count(case when page = '/cart' then 1 end) >= 4
order by latest_event_time
