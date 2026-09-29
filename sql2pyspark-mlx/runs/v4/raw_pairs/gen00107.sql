select * from (select order_id, carrier, cost from shipments) src pivot (avg(cost) for carrier in ('UPS', 'FedEx'));
