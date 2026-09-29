select status, carrier, max(cost) max_cost from shipments where shipped_date between '2023-05-01' and '2025-04-01' group by status, carrier
