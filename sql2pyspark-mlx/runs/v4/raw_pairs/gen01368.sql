select * from (select status, carrier, cost from shipments) pivot (sum(cost) for carrier in ('FedEx', 'Flash'));
