with flattened as (
    -- Unnest the array into one row per element
    select
        c.id,
        c.coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts c
    lateral view outer explode(c.items) e as elem
),
base as (
    -- Keep recent, valid rows
    select
        *,
        array_join(coupon_codes, '|') as coupon_codes_text,
        cast(qty as double) as qty_num,
        coalesce(promo, 'DIRECT') as promo_clean
    from flattened
    where promo is not null
      and price < 80
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        coupon_codes,
        qty,
        price,
        promo,
        coupon_codes_text,
        qty_num,
        promo_clean,
        coalesce(promo, 'Unknown') as promo_filled
    from base
    where promo is not null
),
base_carts as (
    -- Basic cleansing and defaults
    select
        qty,
        promo_clean,
        promo_filled,
        cast(price as int) as price_int,
        coalesce(promo, 'OTHER') as resolved_promo
    from filtered
    where qty_num > 65
),
base_carts_2 as (
    -- Keep recent, valid rows
    select
        *,
        price_int - qty as price_int_minus_qty,
        round(price_int / qty, 2) as price_int_per_qty
    from base_carts
    where qty between 3 and 4
)
-- Multi-level aggregation
select
    case when price_int_per_qty >= 78 then 'HIGH' else 'LOW' end as price_int_per_qty_tier,
    case when price_int_minus_qty >= 65 then 'PREMIUM' else 'STANDARD' end as price_int_minus_qty_tier,
    min(qty) as min_qty,
    round(avg(price_int_minus_qty), 2) as avg_price_int_minus_qty,
    approx_percentile(price_int, 0.95) as price_int_p95
from base_carts_2
group by price_int_per_qty_tier, price_int_minus_qty_tier
