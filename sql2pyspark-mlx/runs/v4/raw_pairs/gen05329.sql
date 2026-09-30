select id, cost, order_id, trim(carrier) as clean_carrier, substring(carrier, 1, 4) as carrier_prefix from shipments where cost > 50 order by cost;
