with exploded as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        inline(items)
    from carts
),
with_windows as (
    select
        id,
        customer_id,
        created_at,
        price,
        promo,
        cume_dist() over (partition by month(created_at), promo order by qty desc nulls last) as qty_cume_dist,
        round(price / sum(price) over (partition by customer_id), 4) as price_share,
        max(price) over (partition by customer_id order by cast(created_at as bigint) asc range between 3600 preceding and current row) as price_max_1h,
        round(product_id / sum(product_id) over (partition by customer_id), 4) as product_id_share,
        dense_rank() over (order by qty desc nulls first) as qty_dense_rank
    from exploded
),
joined as (
    select
        src.customer_id,
        src.created_at,
        src.price,
        src.promo,
        src.qty_cume_dist,
        src.price_share,
        src.price_max_1h,
        c.signup_date,
        c.city
    from with_windows src
    inner join customers c
        on src.customer_id = c.id
),
with_windows_carts as (
    select
        *,
        dense_rank() over w_ordered as qty_cume_dist_dense_rank,
        max(price_share) over (partition by promo) as promo_max_price_share
    from joined
    where price > 200
    window w_ordered as (order by qty_cume_dist desc nulls first)
),
enriched as (
    select
        s.*,
        d.credit_limit,
        d.city as customer_city,
        d.age
    from with_windows_carts s
    left join customers d
        on s.customer_id = d.id
)
select
    c.country,
    c.city as customer_city,
    e.promo,
    count(*) as num_rows,
    sum(e.price_share) as total_price_share
from enriched e
left join customers c on e.customer_id = c.id
group by c.country, c.city, e.promo
order by customer_city desc;
