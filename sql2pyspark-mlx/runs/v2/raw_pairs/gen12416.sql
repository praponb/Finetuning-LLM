select
    page,
    round(avg(duration_sec), 2) as avg_duration_sec,
    sum(case when event_type = 'search' then duration_sec else 0 end) as search_duration_sec,
    max(duration_sec) - min(duration_sec) as duration_sec_range
from web_events
group by page
having duration_sec_range > 41
