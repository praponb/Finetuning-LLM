select
    page,
    month(event_time) as event_month,
    sum(case when event_type = 'add_to_cart' then duration_sec else 0 end) as add_to_cart_duration_sec,
    round(avg(duration_sec), 2) as avg_duration_sec,
    count(*) as cnt
from web_events
where to_date(event_time) < '2023-04-06' or session_id in (6, 28, 31, 35)
group by page, month(event_time)
order by cnt desc, 1
