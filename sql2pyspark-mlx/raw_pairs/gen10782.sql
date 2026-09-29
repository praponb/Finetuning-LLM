with flagged as (
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at asc, id asc) > interval 60 days then 1 else 0 end as is_new_burst
    from sessions
)
select
    id,
    customer_id,
    browser,
    started_at,
    sum(is_new_burst) over (partition by customer_id order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
from flagged;
