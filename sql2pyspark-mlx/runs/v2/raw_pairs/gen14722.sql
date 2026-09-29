select
    store_sales.id,
    store_sales.product_id,
    store_sales.sale_date,
    products.price,
    products.stock_qty
from store_sales
join products on store_sales.product_id = products.id
where array_contains(products.tags, 'premium') or store_sales.product_id in (1, 6, 10)
order by id
limit 10
