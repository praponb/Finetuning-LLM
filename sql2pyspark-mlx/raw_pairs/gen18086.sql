with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.price,
        item.promo
    from carts
    lateral view outer explode(items) li as item
)
-- Running and moving aggregates
select
    id,
    price,
    avg(product_id) over (partition by promo order by cast(created_at as bigint) range between 7200 preceding and current row) as product_id_avg_2h,
    round(product_id / sum(product_id) over (partition by promo), 4) as product_id_share,
    max(price) over (partition by promo order by cast(created_at as bigint) range between 1800 preceding and current row) as price_max_30m
from exploded
where array_contains(coupon_codes, 'SPRING10');
