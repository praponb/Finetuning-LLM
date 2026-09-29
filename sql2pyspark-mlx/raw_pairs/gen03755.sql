with scored as (
    select
        id,
        customer_id,
        order_date,
        channel,
        discount,
        avg(discount) over w_recent as running_avg_discount,
        max(discount) over w_recent as running_max_discount,
        min(discount) over w_recent as running_min_discount,
        count(discount) over w_recent as running_count_discount,
        ntile(5) over (partition by status, channel order by shipping_fee desc, id asc) as shipping_fee_bucket
    from orders
    where discount <= 0.11
    window w_recent as (partition by customer_id order by amount asc, id asc rows between unbounded preceding and current row)
),
filtered as (
    select
        id,
        customer_id,
        channel,
        discount,
        running_avg_discount,
        running_min_discount,
        running_count_discount,
        shipping_fee_bucket,
        round(shipping_fee_bucket * 1.07, 2) as shipping_fee_bucket_adjusted
    from scored
    where customer_id is not null
),
joined as (
    select
        e.id,
        e.customer_id,
        e.channel,
        e.running_min_discount,
        e.shipping_fee_bucket,
        e.shipping_fee_bucket_adjusted,
        p.name,
        p.signup_date,
        p.age
    from filtered e
    left join customers p
        on e.customer_id = p.id
),
metrics as (
    select
        id,
        shipping_fee_bucket,
        shipping_fee_bucket_adjusted,
        name,
        signup_date,
        age,
        percent_rank() over w_ordered as shipping_fee_bucket_percent_rank,
        avg(age) over (partition by year(signup_date), customer_id order by running_min_discount asc, id asc rows between 3 preceding and current row) as age_avg_3
    from joined
    window w_ordered as (partition by year(signup_date), channel order by shipping_fee_bucket desc nulls last)
),
with_latest as (
    select s.*, latest.quantity
    from metrics s
    left join lateral (
            select c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    select
        s.*,
        x.shipment_carrier,
        x.shipped_date
    from with_latest s
    join lateral (
            select c.carrier as shipment_carrier, c.shipped_date
            from shipments c
            where c.order_id = s.id
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    shipping_fee_bucket,
    shipping_fee_bucket_adjusted,
    signup_date,
    age,
    age_avg_3,
    shipment_carrier,
    shipped_date,
    case when shipment_carrier rlike '^(DHL|Kerry)$' then 'GROUP_B' when shipment_carrier rlike '^(FedEx|Flash)$' then 'OTHER_ZONE' else 'PRIORITY' end as shipment_carrier_zone
from with_last
order by shipment_carrier desc, age asc
