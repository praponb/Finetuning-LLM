with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval 60 days then 1 else 0 end as is_new_session
    from payments
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        method,
        paid_at,
        status,
        sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_id
    from gaps
)
select
    id,
    paid_at,
    status
from sessionized
order by paid_at;
