with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        pos,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    from carts
    lateral view posexplode(items) t as pos, li_item
)
-- Window metrics over several different partitions
select
    id,
    customer_id,
    pos,
    product_id,
    qty,
    price,
    max(qty) over (partition by promo order by qty asc, id asc, pos asc rows between unbounded preceding and current row) as running_max_qty,
    avg(qty) over (partition by promo order by qty asc, id asc, pos asc rows between unbounded preceding and current row) as running_avg_qty,
    min(qty) over (partition by promo order by qty asc, id asc, pos asc rows between unbounded preceding and current row) as running_min_qty
from exploded
