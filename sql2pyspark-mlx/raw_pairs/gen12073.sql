select id, shipped_date from shipments where status not in ('LOST', 'IN_TRANSIT') union all select id, shipped_date from shipments where shipped_date >= '2023-11-01'
