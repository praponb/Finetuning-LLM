select * from (select order_id, status, cost from shipments)
pivot (sum(cost) for status in ('IN_TRANSIT', 'RETURNED', 'DELIVERED', 'LOST'))
order by order_id;
