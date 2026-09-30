select p.id, p.method, o.status, o.shipping_fee from payments p join orders o on p.order_id = o.id order by p.id desc, method desc;
