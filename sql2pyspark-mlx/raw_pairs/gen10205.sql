with with_windows as (
    select
        *,
        avg(quantity) over w_hist as running_avg_quantity,
        count(quantity) over w_hist as running_count_quantity,
        min(quantity) over w_hist as running_min_quantity
    from inventory
    window w_hist as (partition by store_id order by quantity, id rows between unbounded preceding and current row)
)
select
    id,
    product_id,
    quantity,
    last_restock,
    running_avg_quantity,
    running_count_quantity
from with_windows
order by quantity desc nulls first, running_avg_quantity;
