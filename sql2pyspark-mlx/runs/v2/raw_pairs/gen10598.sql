select
    p.stock_qty,
    i.quantity,
    i.store_id,
    i.last_restock,
    date_add(p.launch_date, 31) as launch_date_plus_31d
from products p
left join inventory i on p.id = i.product_id
order by p.stock_qty asc, launch_date_plus_31d desc;
