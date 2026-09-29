select p.id, p.stock_qty, p.brand
from products p
where not exists (select 1 from store_sales ss where ss.product_id = p.id and ss.revenue > 2900)
order by id
