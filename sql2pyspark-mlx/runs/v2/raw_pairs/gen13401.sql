select p.status, p.paid_at, p.method, o.store_id from payments p inner join orders o on p.order_id = o.id;
