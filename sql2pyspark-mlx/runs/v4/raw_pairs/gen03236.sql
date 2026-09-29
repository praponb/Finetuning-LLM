with web_events_by_session as (
    select session_id, sort_array(collect_set(page)) as page_list
    from web_events
    group by session_id
)
select s.country, webs.page_list
from sessions s
inner join web_events_by_session webs on s.id = webs.session_id
order by s.country
