with gaps as (
    select *, case when lag(started_at) over (partition by device order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by device order by started_at, id) + interval '30' day then 1 else 0 end as new_group_flag
    from sessions
),
islands as (
    select *, sum(new_group_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
select
    id,
    browser,
    started_at,
    country,
    session_id
from islands;
