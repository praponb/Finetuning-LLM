select
    i.id,
    i.quantity,
    i.product_id,
    i.last_restock,
    ss.store_id,
    greatest(ss.units, ss.revenue) as greatest_units_revenue
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id;
