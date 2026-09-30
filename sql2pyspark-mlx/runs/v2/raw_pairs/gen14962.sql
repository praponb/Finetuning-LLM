select status, carrier from shipments where order_id = 3 union select status, carrier from shipments where order_id is null;
