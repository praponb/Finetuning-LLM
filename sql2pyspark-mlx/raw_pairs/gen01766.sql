with exploded as (
    select
        id,
        created_at,
        coupon_codes,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    from (
        select
            id,
            created_at,
            coupon_codes,
            explode_outer(items) as item
        from carts
    ) e
)
select
    *,
    round(qty / sum(qty) over (partition by promo), 4) as qty_share,
    dense_rank() over (partition by promo order by qty desc) as qty_dense_rank
from exploded
where qty < 3;
