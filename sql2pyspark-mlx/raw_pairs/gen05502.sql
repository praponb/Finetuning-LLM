with flattened as (
    select
        id,
        customer_id,
        coupon_codes,
        inline(items)
    from carts
),
flattened_carts as (
    select
        product_id,
        promo,
        explode(coupon_codes) as tag
    from flattened
),
base as (
    select
        *,
        round(product_id * 0.85, 2) as product_id_scaled,
        coalesce(promo, 'NONE') as promo_filled
    from flattened_carts
    where promo is distinct from 'SPRING10'
),
scored as (
    select
        product_id,
        promo,
        min(product_id_scaled) over (partition by tag) as tag_min_product_id_scaled,
        stddev_samp(product_id) over (partition by tag) as tag_stddev_samp_product_id,
        max(product_id_scaled) over (partition by tag) as tag_max_product_id_scaled
    from base
    where promo is null
)
select
    promo,
    tag_min_product_id_scaled,
    case when promo rlike '^(BUNDLE|WELCOME5)$' then 'GROUP_B' else 'PRIORITY' end as promo_zone
from scored
order by promo_zone desc nulls first;
