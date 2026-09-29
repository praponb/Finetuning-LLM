select a.id, a.status, a.shipping_fee, a.store_id from orders a where a.shipping_fee = (select min(b.shipping_fee) from orders b where b.status = a.status);
