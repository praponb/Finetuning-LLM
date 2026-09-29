select p.launch_date, ss.sale_date
from products p
inner join store_sales ss on ss.product_id = p.id
where ss.revenue > 3200 and array_contains(p.tags, 'eco')
