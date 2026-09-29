with web_events_by_session as (
    select session_id, concat_ws(', ', sort_array(collect_set(page))) as page_values, round((100 * sum(case when page = '/home' then 1 else 0 end)) / count(*), 1) as pct_home
    from web_events
    group by session_id
)
select s.device, webs.page_values, webs.pct_home
from sessions s
inner join web_events_by_session webs on s.id = webs.session_id
order by webs.page_values desc;
