with filtered as (
    select
        *,
        coalesce(discount, 0) as discount_filled,
        add_months(order_date, 1) as order_date_plus_months
    from orders
    where order_date >= date '2023-05-09'
      and customer_id in (2, 18, 21, 32)
),
with_last as (
    select
        s.*,
        top1.order_item_quantity,
        top1.order_item_unit_price
    from filtered s
    left join lateral (
            select c.quantity as order_item_quantity, c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) top1 on true
),
latest as (
    select *
    from with_last
    qualify row_number() over (partition by channel order by order_date desc, id) = 1
),
joined as (
    select a.*, d.store_name
    from latest a
    left join stores d
        on a.store_id = d.id
),
scored as (
    select
        *,
        sum(order_item_quantity) over (order by order_date_plus_months, id rows between unbounded preceding and current row) as cumulative_order_item_quantity,
        lead(shipping_fee, 2) over (partition by channel order by order_date, id) as next_shipping_fee,
        ntile(10) over (partition by channel order by amount, id) as amount_bucket
    from joined
),
with_last_orders as (
    select s.*, x.payment_amount
    from scored s
    join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.paid_at desc, c.id desc
            limit 1
        ) x on true
),
totals as (
    select
        case when discount >= 0.4 then 'PREMIUM' else 'STANDARD' end as discount_tier,
        round(avg(shipping_fee), 2) as avg_shipping_fee,
        max(cumulative_order_item_quantity) filter (where channel = 'web') as web_max_cumulative_order_item_quantity,
        count(distinct store_id) as distinct_store_ids
    from with_last_orders
    group by case when discount >= 0.4 then 'PREMIUM' else 'STANDARD' end
    having avg_shipping_fee < 0
)
select
    discount_tier,
    avg_shipping_fee,
    web_max_cumulative_order_item_quantity,
    distinct_store_ids
from totals
