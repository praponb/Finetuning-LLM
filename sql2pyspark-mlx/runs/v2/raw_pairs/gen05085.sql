select * from (select status, carrier, cost from shipments)
pivot (avg(cost) for carrier in ('FedEx', 'Flash', 'DHL'));
