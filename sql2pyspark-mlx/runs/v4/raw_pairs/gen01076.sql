with with_prev as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by method order by paid_at, id) as prev_paid_at
    from payments
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '90' day then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as island_id
    from gaps
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        prev_paid_at,
        is_new_session,
        island_id,
        last_value(method, true) over (partition by method, status order by prev_paid_at, id rows between unbounded preceding and current row) as last_known_method,
        avg(is_new_session) over (partition by status order by prev_paid_at, id rows between 3 preceding and current row) as is_new_session_avg_3
    from islands
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by status order by amount desc, id) = 1
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            method,
            order_id,
            status,
            is_new_session_avg_3
        from ranked
    ) src
    pivot (
        max(is_new_session_avg_3) as amount,
        count(1) as item_count
        for status in ('SUCCESS' as success, 'REFUNDED' as refunded)
    )
)
select
    method,
    order_id,
    success_amount,
    success_item_count,
    refunded_amount,
    refunded_item_count
from wide
order by order_id, refunded_amount
