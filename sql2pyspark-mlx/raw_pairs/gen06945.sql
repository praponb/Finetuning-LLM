select
    case when page in ('/checkout', '/search') then 'tier_1' else 'other' end as page_group,
    quarter(event_time) as event_quarter,
    max(duration_sec) as max_duration_sec,
    sum(case when page = '/cart' then duration_sec else 0 end) as cart_duration_sec
from web_events
group by page_group, event_quarter;
