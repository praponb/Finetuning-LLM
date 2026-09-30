with filtered_inventory as (
    select *, date_trunc('MONTH', last_restock) as last_restock_period
    from inventory
    where last_restock < '2024-05-24'
),
summary as (
    select date_trunc('MONTH', last_restock) as last_restock_month, avg(quantity) as avg_quantity
    from filtered_inventory
    group by date_trunc('MONTH', last_restock)
)
select *
from summary
order by last_restock_month
