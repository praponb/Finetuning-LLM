with numbered as (
    select
        id,
        event_type,
        duration_sec,
        row_number() over (partition by event_type order by duration_sec, id) as row_num
    from web_events
)
select *
from numbered
where row_num <= 3
order by duration_sec desc
