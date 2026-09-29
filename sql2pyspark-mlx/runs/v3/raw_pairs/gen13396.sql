select store_sales.store_id, date_format(store_sales.sale_date, 'yyyy-MM') as sale_month, max(inventory.quantity) as max_quantity
from inventory
left outer join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where store_sales.store_id in (1, 3)
group by store_sales.store_id, date_format(store_sales.sale_date, 'yyyy-MM');
