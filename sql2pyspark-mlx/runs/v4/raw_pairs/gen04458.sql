with filtered as (
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        round(unit_price, 0) as unit_price_rounded
    from order_items
    where product_id is not null
),
matched_keys as (
    select id, product_id
    from filtered
    intersect
    select id, product_id
    from order_items
    where order_id in (6, 14, 27, 34)
),
verified as (
    select s.*
    from filtered s
    inner join matched_keys k on s.id = k.id and s.product_id = k.product_id
),
with_dims as (
    select
        e.*,
        d.channel,
        d.order_date,
        d.shipping_fee
    from verified e
    left join orders d
        on e.order_id = d.id
)
select
    *,
    rank() over (partition by order_id order by shipping_fee) as shipping_fee_rank,
    rank() over (partition by channel order by unit_price desc nulls last) as unit_price_rank,
    count(shipping_fee) over (partition by product_id) as product_id_count_shipping_fee
from with_dims
