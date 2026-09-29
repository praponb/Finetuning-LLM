select
    i.id,
    i.quantity,
    ss.sale_date,
    last_day(i.last_restock) as last_restock_month_end
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by 3
