with session_summary as (
    select session_id, concat_ws(', ', sort_array(collect_set(page))) as page_values
    from web_events
    group by session_id
)
select sessions.id, sessions.browser, session_summary.page_values
from sessions
inner join session_summary on sessions.id = session_summary.session_id
order by id
