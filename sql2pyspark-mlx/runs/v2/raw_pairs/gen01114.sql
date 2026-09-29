select page, max_duration_sec
from (
    select page, max(duration_sec) as max_duration_sec
    from web_events
    where event_type in ('click', 'search', 'add_to_cart')
    group by page
) as x
where max_duration_sec > 210
order by page
