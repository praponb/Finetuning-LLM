with recent_inventory as (
    select *
    from inventory
    where last_restock <= '2025-04-01'
),
summary as (
    select store_id, date_trunc('MONTH', last_restock) as last_restock_month, count(quantity) as quantity_count
    from recent_inventory
    group by store_id, date_trunc('MONTH', last_restock)
)
select *
from summary
order by last_restock_month, store_id
limit 5;
