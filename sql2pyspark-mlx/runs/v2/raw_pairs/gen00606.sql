select
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock,
    ss.units,
    ss.product_id,
    ss.store_id as store_sale_store_id,
    least(ss.revenue, ss.units) as least_revenue_units
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.quantity > 163;
