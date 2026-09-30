select
    ss.id,
    ss.product_id,
    ss.store_id,
    ss.units
from store_sales ss
where ss.product_id in (select id from products where category = 'Electronics');
