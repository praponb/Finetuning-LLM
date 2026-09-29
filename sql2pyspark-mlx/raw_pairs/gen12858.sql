with exploded as (
    select
        id,
        item.product_id,
        item.qty,
        item.promo
    from carts
    lateral view explode(items) li as item
),
cleaned as (
    select
        *,
        case when promo in ('VIP', 'WELCOME5') then 'Group A' else 'Other' end as promo_group,
        coalesce(promo, 'N/A') as promo_filled
    from exploded
    where promo = 'BUNDLE'
),
with_windows as (
    select
        id,
        product_id,
        qty,
        cume_dist() over (order by qty nulls last) as qty_cume_dist,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share,
        percent_rank() over (order by product_id nulls last) as product_id_percent_rank,
        count(product_id) over (partition by promo) as promo_count_product_id,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share_2
    from cleaned
),
with_windows_carts as (
    select *, rank() over (order by qty_cume_dist) as qty_cume_dist_rank
    from with_windows
),
with_windows_carts_2 as (
    select
        *,
        rank() over (order by qty_cume_dist_rank desc nulls first) as qty_cume_dist_rank_rank,
        rank() over (order by qty_share_2 desc) as qty_share_2_rank,
        cume_dist() over (order by product_id desc nulls last) as product_id_cume_dist
    from with_windows_carts
),
cleaned_carts as (
    select
        product_id,
        qty,
        qty_cume_dist,
        qty_share,
        product_id_percent_rank,
        qty_share_2,
        qty_cume_dist_rank,
        qty_cume_dist_rank_rank,
        qty_share_2_rank,
        cast(qty_share_2_rank as string) as qty_share_2_rank_num,
        qty_share_2_rank - product_id_cume_dist as qty_share_2_rank_minus_product_id_cume_dist
    from with_windows_carts_2
    where qty_cume_dist_rank > 3
),
valid as (
    select
        *,
        qty_share_2_rank + qty_share_2_rank_minus_product_id_cume_dist as qty_share_2_rank_plus_qty_share_2_rank_minus_product_id_cume_dist,
        coalesce(qty_share, 0) as qty_share_filled
    from cleaned_carts
    where qty_share_2_rank_minus_product_id_cume_dist between 26 and 63
)
select
    product_id,
    qty_cume_dist,
    qty_share,
    qty_share_2,
    qty_cume_dist_rank,
    qty_share_2_rank_num,
    qty_share_2_rank_plus_qty_share_2_rank_minus_product_id_cume_dist
from valid
order by qty_share_2 nulls last;
