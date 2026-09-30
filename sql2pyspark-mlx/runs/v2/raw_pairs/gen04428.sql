select * from (select carrier, status, cost from shipments) pivot (avg(cost) for status in ('RETURNED', 'IN_TRANSIT'));
