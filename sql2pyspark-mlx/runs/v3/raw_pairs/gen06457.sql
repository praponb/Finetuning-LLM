with session_totals as (
    select session_id, concat_ws(', ', sort_array(collect_set(page))) as page_values, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    where duration_sec > 358
    group by session_id
)
select
    s.id,
    s.device,
    st.page_values,
    st.avg_duration_sec
from sessions s
inner join session_totals st on s.id = st.session_id
order by device desc
