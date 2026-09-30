with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        case when channel in ('web', 'partner') then 'Group A' else 'Other' end as channel_group,
        coalesce(channel, 'DIRECT') as resolved_channel
    from orders
    where order_date > to_date('2023-08-12')
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from base e
    inner join promotions pr
        on e.channel <=> pr.channel
       and e.order_date between pr.starts_on and pr.ends_on
),
latest_match as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.shipment_carrier,
        x.delivered_date
    from attributed s
    join lateral (
            select c.carrier as shipment_carrier, c.delivered_date
            from shipments c
            where c.order_id = s.id
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
),
with_dims as (
    -- Attach stores attributes
    select
        src.id,
        src.customer_id,
        src.store_id,
        src.channel,
        src.amount,
        src.shipping_fee,
        src.channel_group,
        src.promo_id,
        src.promo_code,
        src.discount_pct,
        src.shipment_carrier,
        d.opened_date
    from latest_match src
    left join stores d
        on src.store_id = d.id
    where d.city = 'New York'
),
latest_match_orders as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.channel,
        s.amount,
        s.shipping_fee,
        s.channel_group,
        s.promo_id,
        s.discount_pct,
        s.opened_date,
        x.payment_paid_at
    from with_dims s
    left join lateral (
            select c.paid_at as payment_paid_at
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    customer_id,
    channel,
    shipping_fee,
    channel_group,
    promo_id,
    opened_date
from latest_match_orders
order by shipping_fee desc nulls last, opened_date desc;
