select o.channel, o.order_date, s.carrier, s.status from orders o left join shipments s on o.id = s.order_id where s.order_id is not null order by order_date desc;
