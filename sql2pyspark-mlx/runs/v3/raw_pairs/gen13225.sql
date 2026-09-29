select * from (select status, carrier, cost from shipments)
pivot (max(cost) for carrier in ('FedEx', 'DHL'))
order by status;
