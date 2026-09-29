select p.id, p.order_id, o.customer_id from payments p full outer join orders o on p.order_id = o.id order by p.id desc;
