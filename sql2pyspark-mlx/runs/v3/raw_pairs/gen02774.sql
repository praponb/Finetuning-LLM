select *, concat(product_name, '/', category) as product_name_category
from products
where supplier_id in (8, 25)
order by supplier_id, brand
