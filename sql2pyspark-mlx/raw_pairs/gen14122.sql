select o.status, p.order_id from orders o left outer join payments p on p.order_id = o.id order by order_id;
