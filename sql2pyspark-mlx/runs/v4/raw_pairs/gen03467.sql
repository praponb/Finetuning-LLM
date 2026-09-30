with lines as (
    select
        id,
        li_item.qty,
        li_item.price
    from carts
    lateral view explode(items) t as li_item
),
cleaned as (
    select *, price + qty as price_plus_qty
    from lines
    where price >= 150
),
enriched as (
    select
        *,
        cume_dist() over w_grp as qty_cume_dist,
        cume_dist() over (order by price) as price_cume_dist
    from cleaned
    window w_grp as (order by qty desc nulls last)
),
kept_keys as (
    select id
    from enriched
    except all
    select id
    from carts
    where to_date(created_at) >= '2024-04-17'
),
reconciled as (
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id
),
with_windows as (
    select
        id,
        qty,
        price,
        price_plus_qty,
        qty_cume_dist,
        price_cume_dist,
        rank() over (order by price_cume_dist desc) as price_cume_dist_rank,
        rank() over (order by qty_cume_dist nulls first) as qty_cume_dist_rank,
        percent_rank() over (order by price nulls last) as price_percent_rank
    from reconciled
),
summary as (
    select
        case when price_cume_dist >= 0.71 then 'LARGE' else 'SMALL' end as price_cume_dist_tier,
        count(*) as num_rows,
        count(*) as cnt,
        count(*) as row_count
    from with_windows
    group by case when price_cume_dist >= 0.71 then 'LARGE' else 'SMALL' end
)
select
    price_cume_dist_tier,
    num_rows,
    cnt,
    row_count
from summary;
