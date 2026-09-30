select id, category, product_name
from products
where supplier_id in (11, 40) and (stock_qty <= 281 or cost < 310)
order by product_name asc
