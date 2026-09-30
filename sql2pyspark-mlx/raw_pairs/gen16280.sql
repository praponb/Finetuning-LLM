with session_totals as (
    select session_id, max(duration_sec) as max_duration_sec
    from web_events
    where page = '/product'
    group by session_id
)
select s.browser, s.customer_id, st.max_duration_sec
from sessions s
left join session_totals st on s.id = st.session_id
where st.max_duration_sec >= 400
order by customer_id, st.max_duration_sec
