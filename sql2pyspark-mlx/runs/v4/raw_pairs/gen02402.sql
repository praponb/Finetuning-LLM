with lines as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        inline(items)
    from carts
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        product_id,
        qty,
        value
    from lines
    lateral view explode(coupon_codes) t as value
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, qty * product_id as qty_times_product_id
    from exploded
    where product_id = 4
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        qty,
        value,
        qty_times_product_id,
        trim(value) as clean_value
    from cleaned
    where value = 'WELCOME5'
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        qty,
        value,
        qty_times_product_id,
        clean_value,
        dense_rank() over w_recent as qty_times_product_id_dense_rank,
        max(qty) over (partition by value) as value_max_qty,
        cume_dist() over (partition by value order by qty asc nulls first) as qty_cume_dist,
        dense_rank() over (order by qty_times_product_id desc nulls first) as qty_times_product_id_dense_rank_2,
        percent_rank() over (order by qty_times_product_id desc) as qty_times_product_id_percent_rank
    from filtered
    window w_recent as (partition by value order by qty_times_product_id desc nulls first)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        avg(qty_times_product_id) over (partition by value) as value_avg_qty_times_product_id,
        cume_dist() over (partition by value order by qty_times_product_id_percent_rank desc) as qty_times_product_id_percent_rank_cume_dist,
        dense_rank() over (partition by value order by qty_times_product_id_percent_rank desc nulls first) as qty_times_product_id_percent_rank_dense_rank,
        rank() over w_ordered as qty_times_product_id_rank,
        percent_rank() over (order by qty_times_product_id desc) as qty_times_product_id_percent_rank_2
    from scored
    window w_ordered as (partition by value order by qty_times_product_id asc nulls last)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, cast(qty_times_product_id_dense_rank as double) as qty_times_product_id_dense_rank_num
    from with_windows
    where qty_times_product_id_rank < 8
)
-- Aggregate with conditional metrics
select
    case when qty_times_product_id_percent_rank_2 >= 0.73 then 'PREMIUM' else 'STANDARD' end as qty_times_product_id_percent_rank_2_tier,
    case when qty_times_product_id_percent_rank_dense_rank >= 7 then 'HIGH' else 'LOW' end as qty_times_product_id_percent_rank_dense_rank_tier,
    approx_percentile(qty_times_product_id_percent_rank_cume_dist, 0.9) as qty_times_product_id_percent_rank_cume_dist_p90,
    count(distinct id) as distinct_ids,
    count_if(value = 'BUNDLE') as bundle_events
from valid
group by qty_times_product_id_percent_rank_2_tier, qty_times_product_id_percent_rank_dense_rank_tier
having approx_percentile(qty_times_product_id_percent_rank_cume_dist, 0.9) >= 0;
