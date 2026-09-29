with cleaned as (
    select
        *,
        round(duration_sec * 1.07, 2) as adjusted_duration_sec,
        date_trunc('year', event_time) as event_time_period
    from web_events
    where event_time >= timestamp '2023-10-12 04:00:00'
)
select *
from (
    select
        event_type,
        page,
        adjusted_duration_sec
    from cleaned
) src
pivot (
    sum(adjusted_duration_sec) as amount
    for page in ('/checkout' as checkout, '/home' as home)
);
