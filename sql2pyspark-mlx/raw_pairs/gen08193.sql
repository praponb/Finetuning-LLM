with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by method order by paid_at, id) > interval 7 days then 1 else 0 end as is_new_session
    from payments
)
-- Running sum of the flag = session number
select
    id,
    order_id,
    method,
    paid_at,
    sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_no
from flagged
