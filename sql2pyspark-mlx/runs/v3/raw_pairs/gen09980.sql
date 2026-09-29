select * from (select carrier, status, cost from shipments where month(delivered_date) in (1, 7, 11)) pivot (count(cost) for status in ('RETURNED', 'DELIVERED', 'IN_TRANSIT')) order by carrier
