select s.carrier, s.shipped_date, o.amount, o.channel from shipments s inner join orders o on s.order_id = o.id where s.cost <= 53;
