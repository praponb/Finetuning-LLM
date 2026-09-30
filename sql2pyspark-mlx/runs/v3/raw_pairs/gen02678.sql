select s.id, s.cost from shipments s where s.cost is not null and s.order_id not in (select id from orders where shipping_fee < 38) order by id
