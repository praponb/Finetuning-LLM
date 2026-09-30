with session_totals as (
    select session_id, sum(case when page = '/cart' then 1 else 0 end) as cart_count
    from web_events
    group by session_id
)
select s.started_at, s.customer_id, st.cart_count
from sessions s
inner join session_totals st on s.id = st.session_id
where st.cart_count > 52
order by s.customer_id asc
