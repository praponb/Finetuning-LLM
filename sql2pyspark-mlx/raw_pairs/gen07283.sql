select
    i.store_id,
    i.last_restock,
    ss.units,
    ss.sale_date,
    ss.store_id as store_sale_store_id,
    round(i.quantity, 0) as quantity_rounded
from inventory i
left outer join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue < 3000 or ss.units between 23 and 29;
