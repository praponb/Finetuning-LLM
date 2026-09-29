select id, shipped_date, cost from shipments where shipped_date between '2023-11-01' and '2024-12-01' order by 3 asc, 1 desc limit 10;
