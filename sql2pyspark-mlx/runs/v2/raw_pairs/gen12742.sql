with src as (
    select *
    from web_events
    where event_type != 'add_to_cart' or duration_sec between 202 and 400
),
page_summary as (
    select page, event_type, count(duration_sec) as duration_sec_count
    from src
    group by page, event_type
)
select page, event_type, duration_sec_count
from page_summary
order by duration_sec_count;
