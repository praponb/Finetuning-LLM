select *
from (
    select date_trunc('MONTH', last_restock) as last_restock_month, round(avg(quantity), 2) as avg_quantity
    from inventory
    group by date_trunc('MONTH', last_restock)
) as sub
where avg_quantity < 56
