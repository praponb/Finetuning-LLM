select store_id, last_restock_month, avg_quantity
from (
    select store_id, date_format(last_restock, 'yyyy-MM') as last_restock_month, avg(quantity) as avg_quantity
    from inventory
    group by store_id, date_format(last_restock, 'yyyy-MM')
) as sub
where avg_quantity >= 160;
