select
    session_id,
    size(collect_set(event_type)) as num_event_types,
    round(sum(duration_sec) / nullif(count(case when page = '/checkout' then 1 end), 0), 2) as duration_sec_per_checkout,
    count(distinct id) as distinct_ids
from web_events
group by session_id
order by distinct_ids desc
