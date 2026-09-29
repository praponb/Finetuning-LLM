select s.id, s.delivered_date, s.order_id from shipments s where s.cost >= 87 and s.order_id in (select id from orders where amount > 440)
