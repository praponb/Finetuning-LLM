select
    i.id,
    i.quantity,
    ss.units,
    ss.sale_date
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id in (1, 2, 3, 5)
order by units desc, quantity desc
