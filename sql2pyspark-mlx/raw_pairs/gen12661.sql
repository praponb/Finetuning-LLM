select payments.method, payments.paid_at, orders.customer_id, customers.name from payments inner join orders on orders.id = payments.order_id inner join customers on orders.customer_id = customers.id
