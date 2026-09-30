with lines as (
    -- Unnest the array into one row per element
    select
        c.id,
        c.customer_id,
        c.created_at,
        c.coupon_codes,
        item.product_id,
        item.price
    from carts c
    lateral view outer explode(c.items) e as item
),
lines_carts as (
    -- Unnest the array into one row per element
    select
        customer_id,
        product_id,
        price,
        code
    from lines
    lateral view explode(coupon_codes) li as code
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        dense_rank() over (partition by code order by product_id desc) product_id_dense_rank,
        rank() over (order by price desc) price_rank,
        dense_rank() over (partition by customer_id, code order by price desc nulls last) price_dense_rank
    from lines_carts
),
filtered as (
    -- Keep recent, valid rows
    select
        product_id,
        price,
        code,
        product_id_dense_rank,
        price_rank,
        price_dense_rank,
        trim(code) clean_code,
        round(product_id_dense_rank, 1) product_id_dense_rank_rounded
    from scored
    where code is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(product_id_dense_rank_rounded) over (partition by code) code_stddev_samp_product_id_dense_rank_rounded,
        min(price_dense_rank) over (partition by code) code_min_price_dense_rank
    from filtered
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(price) over (partition by code) code_stddev_samp_price,
        percent_rank() over (partition by code order by code_stddev_samp_product_id_dense_rank_rounded desc) code_stddev_samp_product_id_dense_rank_rounded_percent_rank
    from enriched
)
select
    product_id,
    price,
    code,
    price_dense_rank,
    clean_code,
    code_min_price_dense_rank,
    code_stddev_samp_product_id_dense_rank_rounded_percent_rank
from metrics
order by price_dense_rank desc nulls last
