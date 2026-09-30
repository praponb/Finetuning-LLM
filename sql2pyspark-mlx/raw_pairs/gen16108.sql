select s.carrier, s.delivered_date, s.status, o.amount, o.order_date, o.customer_id from shipments s left join orders o on o.id = s.order_id;
