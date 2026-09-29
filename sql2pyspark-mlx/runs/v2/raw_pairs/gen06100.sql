select o.order_date, o.discount, s.cost, s.status from orders o left outer join shipments s on s.order_id = o.id;
