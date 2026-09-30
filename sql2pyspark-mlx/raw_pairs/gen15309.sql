with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        shipping_fee,
        round(amount, 2) amount_rounded,
        coalesce(channel, 'DIRECT') resolved_channel
    from orders
    where order_date < '2024-05-01'
),
with_last as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.order_date,
        s.status,
        s.shipping_fee,
        s.amount_rounded,
        s.resolved_channel,
        latest.shipment_status,
        latest.shipment_carrier
    from filtered s
    join lateral (
            select c.status as shipment_status, c.carrier as shipment_carrier
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(shipping_fee) over (partition by date_trunc('MONTH', order_date), status order by order_date, id rows between 6 preceding and current row) shipping_fee_avg_6,
        min(amount_rounded) over (partition by status) status_min_amount_rounded,
        min(shipping_fee) over (partition by status) status_min_shipping_fee
    from with_last
    where status = 'PENDING'
),
valid as (
    -- Basic cleansing and defaults
    select *, status_min_shipping_fee - status_min_amount_rounded status_min_shipping_fee_minus_status_min_amount_rounded
    from with_windows
    where status != 'RETURNED'
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        shipment_carrier,
        shipment_status,
        shipping_fee
    from valid
) src
pivot (
    sum(shipping_fee)
    for shipment_status in ('LOST', 'RETURNED', 'DELIVERED')
);
