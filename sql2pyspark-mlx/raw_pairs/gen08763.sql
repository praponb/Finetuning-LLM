with session_totals as (
    select session_id, count(case when event_type = 'page_view' then 1 end) as page_view_count, max(duration_sec) - min(duration_sec) as duration_sec_range
    from web_events
    where session_id in (7, 31)
    group by session_id
)
select
    s.id,
    s.browser,
    s.customer_id,
    st.page_view_count,
    st.duration_sec_range
from sessions s
left join session_totals st on s.id = st.session_id
where st.duration_sec_range >= 56
order by browser desc
