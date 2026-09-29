select
    i.id,
    i.product_id,
    i.quantity,
    i.store_id,
    ss.units,
    ss.revenue,
    ss.sale_date
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id = 6 and ss.product_id in (4, 7)
order by i.product_id desc
