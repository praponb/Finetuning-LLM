with SessionTotals as (
    select session_id, max(duration_sec) - min(duration_sec) duration_sec_range, count(case when page = '/search' then 1 end) search_count
    from web_events
    group by session_id
)
select
    s.id,
    s.customer_id,
    S.duration_sec_range,
    S.search_count
from sessions s
left join SessionTotals S on s.id = S.session_id
order by S.search_count desc;
