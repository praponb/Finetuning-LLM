with with_prev as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by country order by started_at, id) as prev_started_at
    from sessions
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '90' day then 1 else 0 end as new_group_flag
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.prev_started_at,
    s.new_group_flag,
    s.session_id,
    (select sum(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as sum_order_discount
from islands s;
