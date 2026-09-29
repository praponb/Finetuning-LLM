select products.brand, min(order_items.unit_price) as min_unit_price, max(order_items.quantity) - min(order_items.quantity) as quantity_range
from order_items
left join products on order_items.product_id = products.id
join suppliers on products.supplier_id = suppliers.id
group by products.brand
order by products.brand, quantity_range
