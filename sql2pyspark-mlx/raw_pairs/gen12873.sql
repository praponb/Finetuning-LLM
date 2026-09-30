select quarter(store_sales.sale_date) as sale_quarter, count(inventory.quantity) as quantity_count, max(inventory.last_restock) as latest_last_restock
from inventory
left outer join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by quarter(store_sales.sale_date)
