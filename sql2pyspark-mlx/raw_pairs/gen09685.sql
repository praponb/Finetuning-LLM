select
    i.id,
    i.store_id,
    i.last_restock,
    ss.units,
    date_add(i.last_restock, 7) as last_restock_plus_7d
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by i.id desc
