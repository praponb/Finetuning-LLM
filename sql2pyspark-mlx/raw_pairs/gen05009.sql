with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    from carts
    lateral view explode(items) li as rec
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        qty - product_id as qty_minus_product_id,
        array_contains(coupon_codes, 'VIP') as is_vip,
        coalesce(promo, 'OTHER') as resolved_promo
    from exploded
    where promo is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(qty as string) as qty_num,
        array_join(coupon_codes, ', ') as coupon_codes_text
    from cleaned
    where promo is not distinct from 'VIP'
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        sort_array(coupon_codes) as sorted_coupon_codes,
        coalesce(promo, 'UNKNOWN') as promo_filled
    from base
    where price >= 140
)
select
    coupon_codes,
    product_id,
    qty,
    price,
    promo,
    qty_minus_product_id,
    is_vip,
    qty_num,
    coupon_codes_text
from filtered
order by product_id desc, qty_minus_product_id
