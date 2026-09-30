select s.shipped_date, s.delivered_date, s.order_id, o.shipping_fee, o.amount, o.discount from shipments s left outer join orders o on s.order_id = o.id where s.cost < 73;
