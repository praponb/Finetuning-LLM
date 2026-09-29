select s.* from orders s where exists (select 1 from shipments ch where ch.order_id = s.id and ch.shipped_date >= s.order_date);
