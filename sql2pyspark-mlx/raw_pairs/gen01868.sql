select id, shipped_date, order_id, status, trim(carrier) as clean_carrier from shipments where month(shipped_date) in (8, 11, 12);
