with session_totals as (
    select session_id, round((100 * sum(case when page = '/search' then 1 else 0 end)) / count(*), 1) as pct_search, min(duration_sec) as min_duration_sec
    from web_events
    group by session_id
)
select
    s.id,
    s.started_at,
    st.pct_search,
    st.min_duration_sec
from sessions s
inner join session_totals st on s.id = st.session_id
where st.pct_search >= 37
order by st.pct_search desc;
