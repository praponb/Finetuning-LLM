select orders.id, orders.amount, orders.customer_id from orders where not exists (select 1 from payments where payments.order_id = orders.id) order by orders.amount desc, id desc limit 3
