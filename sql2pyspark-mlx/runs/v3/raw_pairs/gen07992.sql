select
    to_date(event_time) as event_day,
    page,
    sum(case when event_type = 'click' then duration_sec else 0 end) as click_duration_sec,
    min(duration_sec) as min_duration_sec
from web_events
where duration_sec > 446 and duration_sec is null
group by to_date(event_time), page
order by page desc;
