with flagged as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '30' day then 1 else 0 end as session_start_flag
    from payments
),
islands as (
    select *, sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.session_start_flag,
    s.burst_id,
    case when method rlike '^(card|bank_transfer)$' then 'GROUP_B' else 'CORE' end as method_zone,
    (select avg(t2.cost) from shipments t2 where t2.order_id = s.order_id) as avg_shipment_cost
from islands s
order by method_zone
