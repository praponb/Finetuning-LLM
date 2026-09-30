with session_totals as (
    select session_id, round(avg(duration_sec), 2) avg_duration_sec, round((100 * sum(case when event_type = 'purchase' then 1 else 0 end)) / count(*), 1) pct_purchase
    from web_events
    group by session_id
)
select s.started_at, st.avg_duration_sec, st.pct_purchase
from sessions s
join session_totals st on s.id = st.session_id
