select p.method, p.paid_at, p.status, o.amount, o.store_id, o.discount from payments p join orders o on p.order_id = o.id
