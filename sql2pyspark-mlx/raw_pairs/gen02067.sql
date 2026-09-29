with cleaned as (
    select *, greatest(amount, shipping_fee) as greatest_amount_shipping_fee
    from orders
    where order_date >= date '2023-06-17'
),
scored as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        discount,
        sum(greatest_amount_shipping_fee) over w_recent as running_sum_greatest_amount_shipping_fee,
        min(greatest_amount_shipping_fee) over w_recent as running_min_greatest_amount_shipping_fee,
        avg(greatest_amount_shipping_fee) over w_recent as running_avg_greatest_amount_shipping_fee,
        max(greatest_amount_shipping_fee) over w_recent as running_max_greatest_amount_shipping_fee,
        lag(greatest_amount_shipping_fee, 1, 0) over w_recent_2 as prev_greatest_amount_shipping_fee
    from cleaned
    window w_recent as (partition by store_id order by shipping_fee, id rows between unbounded preceding and current row), w_recent_2 as (partition by status order by greatest_amount_shipping_fee, id)
),
latest_match as (
    select
        s.*,
        top1.status,
        top1.payment_method
    from scored s
    left join lateral (
            select c.status, c.method as payment_method
            from payments c
            where c.order_id = s.id
              and c.paid_at <= s.order_date
            order by c.paid_at desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from latest_match s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.unit_price > 230
        )
),
not_flagged_orders as (
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.shipped_date < s.order_date
        )
),
cleaned_orders as (
    select *, greatest(discount, running_min_greatest_amount_shipping_fee) as greatest_discount_running_min_greatest_amount_shipping_fee
    from not_flagged_orders
    where prev_greatest_amount_shipping_fee is not null
)
select
    s.id,
    s.customer_id,
    s.channel,
    s.running_sum_greatest_amount_shipping_fee,
    s.running_max_greatest_amount_shipping_fee,
    s.payment_method,
    case when payment_method rlike '^(card|paypal)$' then 'GROUP_B' when payment_method rlike '^(bank_transfer|cash)$' then 'OTHER_ZONE' else 'CORE' end as payment_method_zone,
    (select avg(o2.discount) from orders o2 where o2.customer_id = s.customer_id) as avg_order_discount
from cleaned_orders s
order by channel;
