with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        greatest(unit_price, quantity) as greatest_unit_price_quantity,
        quantity - unit_price as line_total
    from order_items
    where order_id in (9, 22, 34, 40)
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.line_total,
        b.order_date,
        b.channel,
        b.amount
    from filtered a
    left join orders b
        on a.order_id = b.id
    where b.store_id in (4, 23)
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        b.status,
        b.order_date as order_order_date,
        b.shipping_fee
    from joined src
    left join orders b
        on src.order_id = b.id
),
with_dims_order_items as (
    -- Attach orders attributes
    select e.*, d.channel as order_channel
    from with_dims e
    join orders d
        on e.order_id = d.id
)
-- Native SQL PIVOT
select *
from (
    select
        status,
        order_channel,
        amount
    from with_dims_order_items
) src
pivot (
    max(amount) as total
    for order_channel in ('web' as web, 'mobile' as mobile, 'partner' as partner)
);
