select inventory.product_id, store_sales.revenue, store_sales.revenue * store_sales.units as revenue_per_unit
from inventory
inner join store_sales on inventory.store_id = store_sales.store_id and inventory.product_id = store_sales.product_id
where store_sales.revenue between 2200 and 2600 and inventory.quantity between 76 and 201
order by product_id desc, revenue desc;
