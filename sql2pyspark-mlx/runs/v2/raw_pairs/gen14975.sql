select p.supplier_id, ss.units, ss.revenue
from products p
join store_sales ss on ss.product_id = p.id
