select carrier from shipments where cost is not null union all select carrier from shipments where shipped_date >= '2023-03-01' order by carrier
