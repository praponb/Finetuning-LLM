select p.id, p.status, p.order_id from payments p where p.order_id in (select id from orders where status = 'COMPLETED') order by p.status, order_id desc;
