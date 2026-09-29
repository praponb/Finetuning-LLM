with flattened as (
    select
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.promo
    from carts
    lateral view explode(items) t as elem
),
filtered as (
    select
        qty,
        promo,
        to_date(created_at) as created_at_date,
        cast(qty as double) as qty_str
    from flattened
    where promo is not null
),
scored as (
    select
        *,
        cume_dist() over (order by qty desc nulls last) as qty_cume_dist,
        cume_dist() over (partition by promo order by qty_str desc nulls last) as qty_str_cume_dist,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share
    from filtered
),
enriched as (
    select
        *,
        min(qty_cume_dist) over (partition by promo) as promo_min_qty_cume_dist,
        cume_dist() over (partition by promo order by qty_str desc nulls last) as qty_str_cume_dist_2,
        stddev_samp(qty) over (partition by promo) as promo_stddev_samp_qty
    from scored
)
select
    date_trunc('month', created_at_date) as activity_month,
    max(promo_min_qty_cume_dist) as max_promo_min_qty_cume_dist,
    size(collect_set(promo)) as num_promos,
    sort_array(collect_set(promo)) as promo_list,
    count(*) as event_count
from enriched
group by activity_month
having num_promos < 1
order by activity_month
