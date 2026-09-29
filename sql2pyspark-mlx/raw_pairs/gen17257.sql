select products.price, store_sales.store_id
from products
left join store_sales on products.id = store_sales.product_id
