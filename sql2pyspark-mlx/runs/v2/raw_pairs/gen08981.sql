select p.id, p.method, p.amount, p.order_id from payments p where p.amount between 650 and 1430 and p.order_id in (select id from orders where shipping_fee < 39) order by order_id desc, id limit 10
