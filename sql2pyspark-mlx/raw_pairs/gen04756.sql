select s.*, latest.payment_status from orders s join lateral (select c.status as payment_status from payments c where c.order_id = s.id order by c.paid_at desc, c.id desc limit 1) latest on true;
