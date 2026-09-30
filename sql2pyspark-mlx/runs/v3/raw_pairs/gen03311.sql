select id, carrier, shipped_date, cost, sum(cost) over (partition by carrier order by shipped_date, id) as running_cost from shipments;
