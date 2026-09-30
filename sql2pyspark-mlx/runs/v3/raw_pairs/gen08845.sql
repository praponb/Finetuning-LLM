select o.id, o.shipping_fee from orders o where not exists (select 1 from payments p where p.order_id = o.id);
