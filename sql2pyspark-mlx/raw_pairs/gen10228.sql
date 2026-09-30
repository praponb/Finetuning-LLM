select s.id, s.cost, s.delivered_date, s.order_id from shipments s where s.cost is not null and s.order_id in (select id from orders where store_id is null) order by delivered_date desc;
