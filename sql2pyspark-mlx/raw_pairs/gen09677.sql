select order_id, id from shipments intersect all select order_id, id from shipments where shipped_date >= '2025-01-19'
