select id, order_id from shipments where status in ('LOST', 'DELIVERED', 'IN_TRANSIT') union all select id, order_id from shipments where year(shipped_date) >= 2025;
