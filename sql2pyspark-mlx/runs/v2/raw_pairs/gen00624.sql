select o.id, o.shipping_fee from orders o where o.discount between 0.27 and 0.36 and exists (select 1 from payments p where p.order_id = o.id)
