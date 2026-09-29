with cleaned as (
    select
        id,
        product_id,
        quantity,
        last_restock,
        round(quantity * 1.1, 2) as quantity_scaled,
        day(last_restock) as last_restock_day
    from inventory
    where quantity is not null
)
select
    *,
    min(last_restock_day) over w_ordered as last7_min_last_restock_day,
    max(last_restock_day) over w_ordered as last7_max_last_restock_day,
    sum(last_restock_day) over w_ordered as last7_sum_last_restock_day,
    count(last_restock_day) over w_ordered as last7_count_last_restock_day,
    min(quantity_scaled) over (partition by year(last_restock), product_id) as last_restock_year_min_quantity_scaled
from cleaned
window w_ordered as (partition by product_id order by quantity, id rows between 6 preceding and current row)
