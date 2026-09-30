with ranked as (
    -- QUALIFY-style window filter
    select *
    from shipments
    qualify row_number() over (partition by carrier order by shipped_date desc nulls first, id) = 1
),
with_dims as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.delivered_date,
        e.cost,
        e.status,
        p.amount,
        p.shipping_fee,
        p.status as order_status
    from ranked e
    left join orders p
        on e.order_id = p.id
    where p.store_id = 7
),
with_dims_shipments as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.delivered_date,
        e.cost,
        e.amount,
        e.shipping_fee,
        e.order_status,
        b.order_date,
        b.discount,
        b.shipping_fee as order_shipping_fee
    from with_dims e
    left join orders b
        on e.order_id <=> b.id
),
enriched as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost,
        src.shipping_fee,
        src.order_status,
        src.discount,
        o.discount as order_discount,
        o.amount as order_amount,
        o.order_date as order_order_date
    from with_dims_shipments src
    inner join orders o
        on src.order_id <=> o.id
),
grouped as (
    -- Multi-level aggregation
    select
        order_id,
        date_trunc('year', order_order_date) as year_start,
        max(order_discount) as max_order_discount,
        count(case when order_status = 'COMPLETED' then 1 end) as completed_count,
        percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
        size(collect_set(order_status)) as num_order_status,
        avg(cost) filter (where order_status = 'CANCELLED') as cancelled_avg_cost
    from enriched
    group by order_id, date_trunc('year', order_order_date)
    having size(collect_set(order_status)) >= 1
)
select
    s.order_id,
    s.year_start,
    s.max_order_discount,
    s.cost_quartiles,
    s.num_order_status,
    s.cancelled_avg_cost,
    (select max(t2.amount) from payments t2 where t2.order_id = s.order_id) as max_payment_amount
from grouped s;
