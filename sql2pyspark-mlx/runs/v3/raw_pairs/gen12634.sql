select p.id, p.stock_qty
from products p
where p.launch_date <= '2023-10-01' and exists (select 1 from store_sales ss where ss.product_id = p.id and ss.product_id = 4)
