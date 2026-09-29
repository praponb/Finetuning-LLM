select s.carrier, s.delivered_date, o.status, c.city from shipments s join orders o on s.order_id = o.id right join customers c on o.customer_id = c.id;
