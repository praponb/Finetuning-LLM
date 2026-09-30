select launch_date as event_date from products where cost < 390 intersect select opened_date as event_date from accounts where opened_date between '2024-01-19' and '2024-11-01'
