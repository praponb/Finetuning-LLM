select supplier_id, brand
from products
where length(product_name) >= 8
union
select supplier_id, brand
from products
where stock_qty < 395;
