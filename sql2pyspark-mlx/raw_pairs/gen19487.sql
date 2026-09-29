select store_sales.product_id, store_sales.store_id, min(inventory.quantity) min_quantity
from inventory
left join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where month(store_sales.sale_date) in (4, 8, 11)
group by store_sales.product_id, store_sales.store_id
