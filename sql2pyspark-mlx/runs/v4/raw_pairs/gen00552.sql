with cleaned as (
    select
        id,
        customer_id,
        store_id,
        status,
        amount,
        discount,
        shipping_fee,
        substring(channel, 1, 2) channel_prefix,
        coalesce(channel, 'UNKNOWN') resolved_channel
    from orders
    where order_date >= date '2023-07-31'
      and channel is not null
      and status = 'CANCELLED'
),
with_latest as (
    select s.*, latest.shipment_delivered_date
    from cleaned s
    left join lateral (
            select c.delivered_date as shipment_delivered_date
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    select s.*, x.delivered_date
    from with_latest s
    left join lateral (
            select c.delivered_date
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
)
select
    date_trunc('month', delivered_date) as activity_month,
    max(discount) filter (where status = 'SHIPPED') as shipped_max_discount,
    max(shipping_fee) as max_shipping_fee,
    count(*) filter (where status = 'PENDING') as pending_rows
from with_last
group by date_trunc('month', delivered_date)
having count(*) filter (where status = 'PENDING') > 1
