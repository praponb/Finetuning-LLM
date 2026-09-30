select
    i.id,
    i.last_restock,
    ss.units,
    ss.revenue,
    round(ss.revenue / ss.units, 2) as revenue_per_unit
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by revenue desc;
