with lines as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        key,
        v
    from carts
    lateral view outer explode(attributes) li as key, v
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        sort_array(coupon_codes) as sorted_coupon_codes,
        initcap(trim(key)) as key_title,
        coalesce(key, 'OTHER') as key_clean
    from lines
    where key is distinct from 'device'
),
base as (
    -- Keep recent, valid rows
    select
        coupon_codes,
        key,
        sorted_coupon_codes,
        key_clean,
        array_join(sorted_coupon_codes, '|') as sorted_coupon_codes_text,
        coalesce(key, 'N/A') as key_clean_2
    from filtered
    where key in ('utm_source', 'weight_kg', 'gift')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(key, 1, 2) as key_prefix,
        size(sorted_coupon_codes) as sorted_coupon_codes_count
    from base
    where array_contains(coupon_codes, 'SPRING10')
),
with_windows as (
    -- Ranking and gap features
    select
        coupon_codes,
        sorted_coupon_codes,
        sorted_coupon_codes_text,
        key_prefix,
        sorted_coupon_codes_count,
        stddev_samp(sorted_coupon_codes_count) over w_hist as key_stddev_samp_sorted_coupon_codes_count,
        stddev_samp(sorted_coupon_codes_count) over w_hist as key_stddev_samp_sorted_coupon_codes_count_2
    from cleaned
    window w_hist as (partition by key)
)
select
    coupon_codes,
    sorted_coupon_codes_text,
    key_prefix,
    sorted_coupon_codes_count,
    key_stddev_samp_sorted_coupon_codes_count_2
from with_windows
order by key_stddev_samp_sorted_coupon_codes_count_2 desc nulls last
