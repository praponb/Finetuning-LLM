select a.id, a.status, a.shipping_fee, a.order_date from orders a where a.shipping_fee > (select avg(b.shipping_fee) from orders b where b.status = a.status);
