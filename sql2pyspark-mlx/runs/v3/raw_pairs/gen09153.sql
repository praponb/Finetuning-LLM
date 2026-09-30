select payments.id, payments.status from payments where payments.order_id not in (select id from orders where store_id is not null) order by id desc
