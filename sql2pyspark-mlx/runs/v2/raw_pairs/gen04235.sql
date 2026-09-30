select id, order_id, coalesce(carrier, 'unassigned') as carrier_filled from shipments order by order_id desc, carrier_filled desc;
