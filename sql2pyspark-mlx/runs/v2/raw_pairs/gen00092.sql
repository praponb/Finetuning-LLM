select orders.shipping_fee, orders.discount, payments.method
from orders
inner join payments on payments.order_id = orders.id
where month(orders.order_date) in (2, 7, 8) or payments.method = 'paypal';
