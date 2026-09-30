select id, shipped_date, upper(status) as status_upper from shipments where delivered_date between '2023-08-01' and '2025-10-01';
