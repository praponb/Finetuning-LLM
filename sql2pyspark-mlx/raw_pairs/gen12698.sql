with eligible as (
    select s.*
    from orders s
    where exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.product_id is null
        )
),
latest_match as (
    select
        s.*,
        x.shipment_status,
        x.shipment_cost
    from eligible s
    join lateral (
            select c.status as shipment_status, c.cost as shipment_cost
            from shipments c
            where c.order_id = s.id
              and c.delivered_date <= s.order_date
            order by c.delivered_date desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    select
        *,
        lead(shipment_cost) over w_recent as next_shipment_cost,
        lag(shipment_cost) over w_recent as prev_shipment_cost,
        lag(shipping_fee) over w_recent as prev_shipping_fee,
        row_number() over w_recent as seq_no
    from latest_match
    window w_recent as (partition by customer_id order by amount, id)
),
with_window as (
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from metrics a
    left join promotions pr
        on coalesce(a.channel, 'web') = pr.channel
       and a.order_date >= pr.starts_on
       and a.order_date < pr.ends_on + interval '7' day
),
latest_match_orders as (
    select
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.shipment_status,
        s.shipment_cost,
        s.prev_shipment_cost,
        s.prev_shipping_fee,
        s.promo_id,
        s.promo_code,
        s.discount_pct,
        latest.payment_method,
        latest.payment_status
    from with_window s
    join lateral (
            select c.method as payment_method, c.status as payment_status
            from payments c
            where c.order_id = s.id
            order by c.paid_at desc, c.id desc
            limit 1
        ) latest on true
),
joined as (
    select
        e.*,
        d.country,
        d.age,
        d.name
    from latest_match_orders e
    left join customers d
        on e.customer_id = d.id
    where d.signup_date >= '2024-04-01'
),
joined_orders as (
    select
        a.*,
        b.country as customer_country,
        b.age as customer_age
    from joined a
    left join customers b
        on a.customer_id = b.id
)
select
    id,
    status,
    channel,
    shipping_fee,
    shipment_status,
    prev_shipment_cost,
    prev_shipping_fee,
    promo_id,
    promo_code
from joined_orders
order by prev_shipment_cost, prev_shipping_fee;
