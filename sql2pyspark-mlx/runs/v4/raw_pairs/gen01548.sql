select *
from (
    select
        date_trunc('MONTH', event_time) as event_month,
        session_id,
        round((100 * sum(case when event_type = 'purchase' then 1 else 0 end)) / count(*), 1) as pct_purchase,
        round((100 * sum(case when page = '/home' then 1 else 0 end)) / count(*), 1) as pct_home
    from web_events
    where session_id = 2
    group by date_trunc('MONTH', event_time), session_id
) as t
where pct_purchase > 25
order by pct_purchase desc;
