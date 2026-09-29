with flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        customer_id,
        coupon_codes,
        attributes,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        exists(items, i -> i.price > 100) as has_premium_item,
        aggregate(items, 0D, (acc, i) -> acc + i.qty * i.price, acc -> round(acc, 2)) as cart_value,
        forall(items, i -> i.qty <= 5) as all_small_qty,
        transform(
            filter(items, i -> i.promo is not null),
            i -> struct(i.product_id as pid, round(i.price * 0.9, 2) as discounted_price)
        ) as promo_lines
    from carts
)
-- Window metrics over several different partitions
select
    id,
    customer_id,
    coupon_codes,
    weight_kg,
    has_premium_item,
    ntile(10) over (partition by customer_id order by weight_kg desc, id) as weight_kg_bucket,
    row_number() over (order by weight_kg, id) as event_seq
from flattened_attrs
