select
    i.id,
    i.last_restock,
    ss.units,
    ss.units * ss.revenue as revenue_per_unit
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.units is not null
order by i.id, revenue_per_unit
