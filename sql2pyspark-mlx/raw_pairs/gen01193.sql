select
    store_sales.id,
    store_sales.product_id,
    store_sales.revenue,
    products.price,
    stores.city,
    datediff(current_date(), store_sales.sale_date) / 365.25 as years_since_sale_date
from store_sales
inner join products on store_sales.product_id = products.id
left join stores on store_sales.store_id = stores.id
