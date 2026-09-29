select status, shipped_date from shipments where delivered_date < '2023-09-14' intersect select status, shipped_date from shipments where status in ('DELIVERED', 'RETURNED')
