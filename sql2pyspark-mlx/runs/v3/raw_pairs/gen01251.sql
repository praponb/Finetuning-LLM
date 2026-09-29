select * from (select status, carrier, cost from shipments) pivot (count(cost) for carrier in ('UPS', 'FedEx', 'Flash'));
