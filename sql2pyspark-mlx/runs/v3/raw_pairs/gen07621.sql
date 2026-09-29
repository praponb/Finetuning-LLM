select id, cost
from products
where (supplier_id is null and stock_qty <> 317) or cost >= 540
