select p.id, p.method, o.customer_id, o.amount from payments p join orders o on o.id = p.order_id
