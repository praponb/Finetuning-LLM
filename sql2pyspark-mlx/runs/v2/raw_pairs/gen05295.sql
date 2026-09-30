select id, cost, shipped_date from shipments where month(shipped_date) in (3, 4, 5) and cost < 47 and delivered_date between '2024-09-27' and '2025-08-01';
