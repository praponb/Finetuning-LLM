with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 90 days then 1 else 0 end as is_new_session
    from payments
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        sum(is_new_session) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from flagged
)
select
    id,
    amount,
    paid_at,
    status,
    island_id
from islands
order by id;
