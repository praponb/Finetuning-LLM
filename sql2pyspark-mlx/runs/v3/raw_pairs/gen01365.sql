select
    store_sales.revenue,
    products.product_name,
    suppliers.rating,
    floor(products.price) price_whole
from store_sales
join products on store_sales.product_id = products.id
full outer join suppliers on products.supplier_id = suppliers.id
order by products.product_name, price_whole
