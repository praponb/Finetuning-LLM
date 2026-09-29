select
    store_sales.id,
    store_sales.revenue,
    stores.city,
    stores.store_name,
    products.cost
from store_sales
inner join stores on store_sales.store_id = stores.id
inner join products on store_sales.product_id = products.id
where stores.city in ('New York', 'Chiang Mai');
