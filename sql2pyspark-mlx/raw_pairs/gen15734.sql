with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        hour(event_time) as event_time_hour
    from web_events
    where page != '/checkout'
)
-- Join a dimension and aggregate
select
    dim.country,
    s.event_type,
    count(*) as rows_total,
    avg(s.duration_sec) as avg_duration_sec,
    grouping_id() as level_id
from cleaned s
inner join sessions dim on s.session_id = dim.id
group by grouping sets ((dim.country, s.event_type), (dim.country))
order by country nulls first;
