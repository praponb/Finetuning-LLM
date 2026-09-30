select stores.city, min(orders.order_date) as min_order_date, max(orders.shipping_fee) - min(orders.shipping_fee) as shipping_fee_range
from orders
right outer join stores on orders.store_id = stores.id
left outer join customers on orders.customer_id = customers.id
group by stores.city;
