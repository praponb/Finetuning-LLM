with flagged as (
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval 7 days then 1 else 0 end as is_new_burst
    from sessions
)
select *, sum(is_new_burst) over (partition by device order by started_at, id rows between unbounded preceding and current row) as burst_id
from flagged;
