select * from (select carrier, status, cost from shipments where order_id is not null)
pivot (avg(cost) for status in ('DELIVERED', 'RETURNED'))
