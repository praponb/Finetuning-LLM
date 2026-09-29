select
    i.store_id,
    ss.store_id as store_sale_store_id,
    ss.sale_date,
    ss.product_id,
    round(i.quantity, 2) as quantity_rounded
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
order by 5, ss.product_id desc;
