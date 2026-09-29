with with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.quantity,
        x.order_item_unit_price
    from orders s
    join lateral (
            select c.quantity, c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        first_value(amount, true) over (order by discount, id rows between unbounded preceding and current row) as first_known_amount,
        lag(discount, 1) over (partition by store_id, customer_id order by order_item_unit_price, id) as prev_discount,
        max(discount) over (partition by channel) as channel_max_discount,
        cume_dist() over (partition by channel order by order_item_unit_price desc nulls last) as order_item_unit_price_cume_dist,
        row_number() over (partition by customer_id order by order_item_unit_price desc, id) as seq_no
    from with_last
),
latest_match as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, x.payment_amount
    from with_windows s
    left join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.paid_at desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest_match
    qualify rank() over (partition by store_id order by order_item_unit_price_cume_dist) = 1
)
select
    id,
    customer_id,
    store_id,
    order_date,
    amount,
    shipping_fee,
    order_item_unit_price,
    first_known_amount,
    prev_discount
from ranked
order by prev_discount desc, first_known_amount
