select
    ss.id,
    ss.units,
    ss.sale_date,
    ss.store_id
from store_sales ss
where ss.product_id in (2, 3, 4, 9) and ss.product_id not in (select id from products where supplier_id is not null)
