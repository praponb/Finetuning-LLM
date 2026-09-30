with cleaned as (
    select *, year(last_restock) as last_restock_year
    from inventory
    where store_id in (3, 4, 5)
),
joined as (
    select
        e.id,
        e.product_id,
        e.quantity,
        p.brand,
        p.launch_date,
        p.product_name
    from cleaned e
    left join products p
        on e.product_id = p.id
    where p.price >= 670
),
enriched as (
    select
        id,
        product_id,
        quantity,
        product_name,
        sum(quantity) over (partition by year(launch_date), product_id order by launch_date asc, id asc rows between unbounded preceding and current row) as cumulative_quantity,
        sum(quantity) over (partition by brand order by quantity asc, id asc rows between unbounded preceding and current row) as running_quantity,
        stddev_samp(quantity) over (partition by year(launch_date), product_id) as launch_date_year_stddev_samp_quantity
    from joined
),
joined_inventory as (
    select
        src.id,
        src.product_id,
        src.quantity,
        src.running_quantity,
        b.price,
        b.brand,
        b.category
    from enriched src
    join products b
        on src.product_id = b.id
)
select
    id,
    product_id,
    quantity,
    running_quantity,
    brand,
    avg(running_quantity) over w_grp as running_avg_running_quantity,
    sum(running_quantity) over w_grp as running_sum_running_quantity,
    count(running_quantity) over w_grp as running_count_running_quantity,
    lag(running_quantity, 1) over (partition by brand order by price asc, id asc) as prev_running_quantity
from joined_inventory
window w_grp as (partition by brand, category order by quantity asc, id asc rows between unbounded preceding and current row);
