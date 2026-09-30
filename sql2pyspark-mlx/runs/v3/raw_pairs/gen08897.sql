select suppliers.city, max(order_items.unit_price) - min(order_items.unit_price) as unit_price_range, round(stddev(order_items.quantity), 2) as stddev_quantity
from order_items
left join products on order_items.product_id = products.id
inner join suppliers on products.supplier_id = suppliers.id
where suppliers.rating is null
group by suppliers.city;
