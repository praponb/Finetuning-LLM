select
    store_sales.id,
    store_sales.product_id,
    products.price,
    products.stock_qty,
    stores.region
from store_sales
left join products on products.id = store_sales.product_id
left join stores on stores.id = store_sales.store_id
where store_sales.units > 37
order by 2 desc
