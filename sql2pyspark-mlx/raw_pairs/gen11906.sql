with gaps as (
    select *, case when started_at - lag(started_at) over (partition by country order by started_at asc, id asc) > interval '14' day then 1 else 0 end as is_new_session
    from sessions
)
select
    id,
    device,
    browser,
    started_at,
    country,
    sum(is_new_session) over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
from gaps;
