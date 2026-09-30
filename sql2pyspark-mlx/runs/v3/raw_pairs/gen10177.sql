with web_events_by_session as (
    select session_id, sum(case when event_type = 'purchase' then duration_sec else 0 end) as purchase_duration_sec
    from web_events
    group by session_id
)
select s.browser, s.device, webs.purchase_duration_sec
from sessions s
inner join web_events_by_session webs on s.id = webs.session_id
where webs.purchase_duration_sec > 46
order by webs.purchase_duration_sec desc;
