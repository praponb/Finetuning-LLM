select id, delivered_date, cost, round(cost, 1) as cost_rounded from shipments where month(shipped_date) in (1, 5, 10);
