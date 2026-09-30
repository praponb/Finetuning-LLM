select
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock,
    ss.units,
    date_format(i.last_restock, 'yyyy-MM') as last_restock_label
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by i.last_restock desc;
