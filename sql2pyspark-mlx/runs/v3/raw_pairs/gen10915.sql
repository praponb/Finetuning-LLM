with base as (
    select *
    from web_events
    where page in ('/product', '/search')
),
EventTypeSummary as (
    select event_type, date_format(event_time, 'yyyy-MM') as event_month, count(*) as row_count
    from base
    group by event_type, date_format(event_time, 'yyyy-MM')
)
select *
from EventTypeSummary
where row_count >= 3
order by row_count desc
