select
    products.id,
    products.brand,
    products.price,
    products.cost
from products
where array_contains(products.tags, 'eco') and exists (select 1 from store_sales where store_sales.product_id = products.id and store_sales.store_id in (1, 5))
