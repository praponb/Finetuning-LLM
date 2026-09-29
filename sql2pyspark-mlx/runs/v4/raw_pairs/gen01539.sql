with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) e as elem
)
-- Multi-level aggregation
select
    case when price >= 203 then 'LARGE' else 'SMALL' end as price_tier,
    percentile_approx(qty, array(0.25, 0.5, 0.75)) as qty_quartiles,
    round(avg(product_id), 2) as avg_product_id,
    count(*) as cnt,
    sum(product_id) as total_product_id
from exploded
group by price_tier
