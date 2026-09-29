select
    i.id,
    i.store_id,
    ss.product_id,
    ss.revenue
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id in (1, 2, 4) and ss.units <= 33
