with filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        amount,
        paid_at,
        substring(status, 1, 4) as status_prefix,
        coalesce(method, 'UNKNOWN') as method_filled
    from payments
    where amount >= 920
),
prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from filtered
),
flagged as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '30' day then 1 else 0 end as is_new_session
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        b.discount,
        b.amount as order_amount
    from sessionized s
    left join orders b
        on s.order_id = b.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at, id) > interval '30' day then 1 else 0 end as new_group_flag
    from with_dims
),
sessionized_payments as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id_2
    from with_gaps
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by amount nulls first, id) as latest_rank
        from sessionized_payments
    ) ranked
    where latest_rank = 1
)
select
    s.id,
    s.order_id,
    s.status_prefix,
    s.method_filled,
    s.prev_paid_at,
    s.burst_id,
    s.discount,
    s.order_amount,
    s.new_group_flag,
    (select avg(x.cost) from shipments x where x.order_id = s.order_id) as avg_shipment_cost
from deduped s
order by avg_shipment_cost desc;
