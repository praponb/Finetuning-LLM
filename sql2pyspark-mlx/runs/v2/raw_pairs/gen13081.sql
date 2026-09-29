select
    i.product_id,
    i.last_restock,
    i.quantity,
    ss.sale_date,
    add_months(ss.sale_date, 12) as sale_date_plus_months
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id in (4, 6);
