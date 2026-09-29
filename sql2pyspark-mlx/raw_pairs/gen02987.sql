with exploded as (
    -- Unnest the array into one row per element
    select
        c.id,
        c.coupon_codes,
        line.product_id,
        line.qty,
        line.price
    from carts c
    lateral view outer explode(c.items) e as line
),
scored as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by product_id desc nulls first) as product_id_rank,
        percent_rank() over w_ordered as price_percent_rank
    from exploded
    where product_id != 4
    window w_ordered as (order by price desc)
),
filtered as (
    -- Basic cleansing and defaults
    select *, greatest(price_percent_rank, price) as greatest_price_percent_rank_price
    from scored
    where qty between 3 and 4
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        greatest_price_percent_rank_price * price_percent_rank as greatest_price_percent_rank_price_times_price_percent_rank,
        sort_array(coupon_codes) as sorted_coupon_codes
    from filtered
    where array_contains(coupon_codes, 'SPRING10')
)
-- Running and moving aggregates
select
    id,
    coupon_codes,
    product_id,
    qty,
    price,
    product_id_rank,
    price_percent_rank,
    greatest_price_percent_rank_price_times_price_percent_rank,
    sorted_coupon_codes,
    dense_rank() over (order by greatest_price_percent_rank_price_times_price_percent_rank desc nulls first) as greatest_price_percent_rank_price_times_price_percent_rank_dense_rank,
    rank() over (order by product_id_rank desc nulls last) as product_id_rank_rank,
    dense_rank() over (order by product_id_rank desc) as product_id_rank_dense_rank,
    dense_rank() over (order by greatest_price_percent_rank_price_times_price_percent_rank desc nulls last) as greatest_price_percent_rank_price_times_price_percent_rank_dense_rank_2,
    percent_rank() over (order by product_id_rank desc nulls first) as product_id_rank_percent_rank
from valid;
