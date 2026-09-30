select * from (select carrier, status, cost from shipments) pivot (max(cost) for status in ('IN_TRANSIT', 'LOST'));
