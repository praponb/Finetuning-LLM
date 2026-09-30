select p.id, p.amount, p.status from payments p where p.order_id in (select id from orders where status = 'SHIPPED')
