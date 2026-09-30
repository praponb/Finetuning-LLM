select ss.id, ss.store_id, ss.product_id
from store_sales ss
where ss.sale_date > '2025-10-12' and ss.product_id not in (select id from products where category = 'Fashion')
order by id
