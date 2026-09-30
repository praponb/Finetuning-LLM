select products.id, products.brand
from products
where not exists (select 1 from store_sales where store_sales.product_id = products.id and store_sales.units < 28)
