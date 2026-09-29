select o.amount, o.customer_id, o.order_date, s.cost from orders o left join shipments s on o.id = s.order_id where s.cost < 68;
