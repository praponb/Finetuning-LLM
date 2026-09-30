select id, order_id, carrier, status from shipments where carrier not in ('DHL', 'Flash', 'Kerry') or status in ('IN_TRANSIT', 'DELIVERED', 'LOST') order by order_id desc, id asc limit 10;
