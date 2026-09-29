with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        entry.qty,
        entry.price,
        entry.promo
    from carts
    lateral view outer explode(items) t as entry
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share,
        sum(qty) over (order by cast(created_at as bigint) asc range between 3600 preceding and current row) as qty_sum_1h,
        cume_dist() over (partition by date_trunc('MONTH', created_at) order by qty desc nulls last) as qty_cume_dist
    from exploded
    where array_contains(coupon_codes, 'SPRING10')
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(price as int) as price_int
    from with_windows
    where created_at >= timestamp '2025-03-02 00:00:00'
      and promo is not null
),
with_windows_carts as (
    -- Running and moving aggregates
    select
        created_at,
        coupon_codes,
        price,
        price_int,
        rank() over (partition by promo order by qty desc) as qty_rank,
        stddev_samp(qty_sum_1h) over (partition by promo) as promo_stddev_samp_qty_sum_1h,
        cume_dist() over (partition by promo order by qty_sum_1h desc nulls last) as qty_sum_1h_cume_dist
    from filtered
),
metrics as (
    -- Running and moving aggregates
    select
        created_at,
        coupon_codes,
        qty_rank,
        qty_sum_1h_cume_dist,
        sum(qty_sum_1h_cume_dist) over (order by cast(created_at as bigint) asc range between 3600 preceding and current row) as qty_sum_1h_cume_dist_sum_1h
    from with_windows_carts
)
-- Grouped metrics
select
    date_trunc('hour', created_at) as hour_window,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    max(created_at) as last_created_at
from metrics
group by date_trunc('hour', created_at)
order by hour_window desc nulls last
