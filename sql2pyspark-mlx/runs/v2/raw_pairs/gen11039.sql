select payments.status, payments.paid_at, orders.order_date from payments inner join orders on payments.order_id = orders.id where orders.shipping_fee < 34;
