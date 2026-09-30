with base as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        quantity,
        unit_price,
        cast(unit_price as int) unit_price_int
    from order_items
    where product_id is not null
      and product_id in (10, 11, 16)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(unit_price) over w_recent order_id_max_unit_price,
        lead(quantity, 2) over w_grp next_quantity
    from base
    window w_recent as (partition by order_id), w_grp as (partition by order_id order by unit_price, id)
),
joined as (
    -- Attach orders attributes
    select
        e.*,
        p.shipping_fee,
        p.channel
    from scored e
    left join orders p
        on e.order_id = p.id
),
with_dims as (
    -- Attach orders attributes
    select
        e.*,
        b.channel as order_channel,
        b.amount
    from joined e
    left join orders b
        on e.order_id = b.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        quantity,
        unit_price,
        unit_price_int,
        order_id_max_unit_price,
        next_quantity,
        shipping_fee,
        channel,
        order_channel,
        amount,
        count(amount) over (partition by order_channel) order_channel_count_amount,
        percent_rank() over (partition by channel order by shipping_fee desc nulls first) shipping_fee_percent_rank
    from with_dims
),
joined_order_items as (
    -- Attach orders attributes
    select
        s.*,
        p.amount as order_amount,
        p.status
    from enriched s
    join orders p
        on s.order_id = p.id
)
select
    id,
    order_id,
    quantity,
    unit_price,
    order_id_max_unit_price,
    next_quantity,
    shipping_fee,
    channel,
    order_channel
from joined_order_items;
