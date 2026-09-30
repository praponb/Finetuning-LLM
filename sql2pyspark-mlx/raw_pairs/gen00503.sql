select id, shipped_date, cost, lead(cost, 1) over (order by shipped_date, id) as next_cost from shipments;
