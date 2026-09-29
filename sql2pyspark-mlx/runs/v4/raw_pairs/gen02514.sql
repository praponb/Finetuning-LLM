select
    store_sales.product_id,
    stores.store_name,
    products.price,
    products.supplier_id,
    last_day(store_sales.sale_date) sale_date_month_end
from store_sales
right join stores on store_sales.store_id = stores.id
join products on store_sales.product_id = products.id
where store_sales.product_id = 6;
