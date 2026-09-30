select s.id, s.shipped_date, s.delivered_date, s.cost, o.status, o.channel from shipments s inner join orders o on o.id = s.order_id order by s.shipped_date, s.cost desc, id limit 10;
