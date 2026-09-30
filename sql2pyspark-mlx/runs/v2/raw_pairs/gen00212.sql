select
    store_sales.revenue,
    products.product_name,
    stores.store_name,
    stores.region,
    case when products.category = 'Sports' then 1 else 0 end as is_sports
from store_sales
inner join products on products.id = store_sales.product_id
inner join stores on store_sales.store_id = stores.id;
