select p.id, p.status, p.method, p.paid_at, o.status as order_status from payments p left join orders o on p.order_id = o.id
