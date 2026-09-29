select
    store_sales.id,
    store_sales.revenue,
    store_sales.product_id,
    store_sales.sale_date,
    products.category
from store_sales
inner join products on store_sales.product_id = products.id
