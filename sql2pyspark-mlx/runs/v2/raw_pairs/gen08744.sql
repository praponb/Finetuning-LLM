select s.id, s.cost, o.channel, o.shipping_fee from shipments s right join orders o on s.order_id = o.id where o.channel <> 'mobile' order by id desc, cost;
