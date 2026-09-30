select o.status, min(p.paid_at) as min_paid_at from payments p inner join orders o on p.order_id = o.id inner join stores s on o.store_id = s.id group by o.status;
