select shipped_date, delivered_date from shipments where cost between 33 and 90 union all select shipped_date, delivered_date from shipments where order_id = 34;
