select delivered_date, shipped_date from shipments where status = 'DELIVERED' and year(delivered_date) = 2025 and status <> 'LOST'
