select orders.id, orders.discount from orders where orders.customer_id in (select id from customers where age < 45) order by discount
