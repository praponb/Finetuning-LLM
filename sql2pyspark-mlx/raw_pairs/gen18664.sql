with flattened as (
    -- Unnest the array into one row per element
    select
        t.id,
        t.customer_id,
        item.product_id,
        item.price,
        item.promo
    from carts t
    lateral view explode(t.items) li as item
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        promo,
        cast(price as int) as price_int,
        coalesce(promo, 'N/A') as promo_clean
    from flattened
    where customer_id = 10
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        substring(promo, 1, 2) as promo_prefix,
        coalesce(promo, 'NONE') as resolved_promo
    from valid
    where price_int between 33 and 42
),
filtered_carts as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        promo,
        promo_clean,
        promo_prefix,
        resolved_promo,
        coalesce(product_id, 0) as product_id_filled
    from filtered
    where promo is distinct from 'VIP'
      and price_int <= 31
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        promo,
        promo_clean,
        resolved_promo,
        product_id_filled,
        product_id * product_id_filled as product_id_times_product_id_filled
    from filtered_carts
    where promo is not null
      and product_id < 9
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        promo_clean,
        product_id_times_product_id_filled,
        stddev_samp(product_id_times_product_id_filled) over (partition by promo) as promo_stddev_samp_product_id_times_product_id_filled,
        stddev_samp(product_id_filled) over (partition by promo) as promo_stddev_samp_product_id_filled,
        rank() over (order by product_id_filled desc) as product_id_filled_rank,
        sum(product_id_times_product_id_filled) over (partition by promo) as promo_sum_product_id_times_product_id_filled
    from cleaned
    where product_id_times_product_id_filled > 54
)
-- Aggregate with conditional metrics
select
    product_id,
    count(*) as row_count,
    percentile_approx(promo_stddev_samp_product_id_filled, array(0.25, 0.5, 0.75)) as promo_stddev_samp_product_id_filled_quartiles,
    percentile_approx(product_id_filled_rank, 0.9) as product_id_filled_rank_p90
from enriched
group by 1;
