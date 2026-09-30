select ss.id, ss.sale_date
from store_sales ss
where ss.units >= 12 and ss.product_id not in (select id from products where array_contains(tags, 'new'))
order by id
