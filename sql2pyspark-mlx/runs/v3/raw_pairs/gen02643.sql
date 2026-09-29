select products.category, sum(order_items.unit_price) as total_unit_price, count(distinct order_items.quantity) as unique_orders
from order_items
left join products on order_items.product_id = products.id
join suppliers on products.supplier_id = suppliers.id
group by products.category
order by total_unit_price, products.category
