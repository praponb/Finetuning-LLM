with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    from carts
    lateral view explode(items) li as line
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        created_at,
        coupon_codes,
        product_id,
        qty,
        price,
        max(price) over (order by cast(created_at as bigint) range between 3600 preceding and current row) as price_max_1h,
        max(qty) over (order by cast(created_at as bigint) range between 7200 preceding and current row) as qty_max_2h,
        sum(qty) over (order by cast(created_at as bigint) range between 1800 preceding and current row) as qty_sum_30m
    from lines
),
scored_carts as (
    -- Window metrics over several different partitions
    select
        *,
        max(qty_sum_30m) over w_running as qty_sum_30m_max_1h,
        avg(product_id) over (order by cast(created_at as bigint) range between 1800 preceding and current row) as product_id_avg_30m,
        dense_rank() over (order by price desc nulls first) as price_dense_rank
    from scored
    window w_running as (order by cast(created_at as bigint) range between 3600 preceding and current row)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(price_max_1h, 0) as price_max_1h_rounded,
        round(qty_sum_30m_max_1h, 2) as qty_sum_30m_max_1h_rounded
    from scored_carts
    where created_at > timestamp '2025-03-02 02:00:00'
),
scored_carts_2 as (
    -- Running and moving aggregates
    select
        *,
        max(qty_sum_30m_max_1h_rounded) over w_hist as qty_sum_30m_max_1h_rounded_max_2h,
        sum(qty) over (order by cast(created_at as bigint) range between 900 preceding and current row) as qty_sum_15m
    from filtered
    window w_hist as (order by cast(created_at as bigint) range between 7200 preceding and current row)
),
valid as (
    -- Basic cleansing and defaults
    select *, price_max_1h - qty_sum_30m_max_1h_rounded as price_max_1h_minus_qty_sum_30m_max_1h_rounded
    from scored_carts_2
    where created_at > timestamp '2025-03-02 13:00:00'
      and qty_sum_30m_max_1h <= 10
),
matched_keys as (
    -- Set difference on keys
    select id
    from valid
    except distinct
    select id
    from carts
    where customer_id = 40
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.id = k.id
)
-- Window metrics over several different partitions
select
    id,
    created_at,
    coupon_codes,
    product_id,
    qty,
    price,
    price_max_1h,
    qty_max_2h,
    qty_sum_30m,
    qty_sum_30m_max_1h,
    product_id_avg_30m,
    price_dense_rank,
    price_max_1h_rounded,
    qty_sum_30m_max_1h_rounded,
    qty_sum_30m_max_1h_rounded_max_2h,
    qty_sum_15m,
    price_max_1h_minus_qty_sum_30m_max_1h_rounded,
    cume_dist() over (order by price_max_1h_rounded) as price_max_1h_rounded_cume_dist,
    avg(qty_sum_30m) over (order by cast(created_at as bigint) range between 3600 preceding and current row) as qty_sum_30m_avg_1h,
    avg(qty_sum_15m) over (order by cast(created_at as bigint) range between 7200 preceding and current row) as qty_sum_15m_avg_2h,
    sum(price_max_1h_minus_qty_sum_30m_max_1h_rounded) over (order by cast(created_at as bigint) range between 900 preceding and current row) as price_max_1h_minus_qty_sum_30m_max_1h_rounded_sum_15m
from verified
