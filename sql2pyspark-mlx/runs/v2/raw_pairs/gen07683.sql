select o.id, o.channel, count(s.id) shipment_count, count(distinct s.cost) unique_carriers, count(s.cost) cost_count from orders o join shipments s on o.id = s.order_id group by o.id, o.channel;
