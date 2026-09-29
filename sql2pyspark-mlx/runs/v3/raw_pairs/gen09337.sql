select * from (select carrier, status, cost from shipments) pivot (max(cost) for status in ('LOST', 'RETURNED', 'IN_TRANSIT')) order by carrier
