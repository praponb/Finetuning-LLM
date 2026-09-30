select quarter(store_sales.sale_date) sale_quarter, round(avg(inventory.quantity), 2) avg_quantity, max(inventory.last_restock) last_last_restock
from inventory
join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
group by quarter(store_sales.sale_date);
