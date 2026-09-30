with session_stats as (
    select session_id, count(case when page = '/checkout' then 1 end) as checkout_count
    from web_events
    where event_time >= '2024-05-01'
    group by session_id
)
select s.id, s.browser, ss.checkout_count
from sessions s
left outer join session_stats ss on s.id = ss.session_id
order by checkout_count desc
