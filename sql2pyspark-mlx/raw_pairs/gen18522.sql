select
    store_sales.units,
    store_sales.store_id,
    store_sales.revenue,
    products.supplier_id,
    products.product_name
from store_sales
join products on store_sales.product_id = products.id
