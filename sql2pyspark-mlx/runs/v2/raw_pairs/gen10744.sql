select carrier, sum(cost) as total_cost, sum(sum(cost)) over (order by carrier) as cumulative_cost from shipments where delivered_date >= '2025-09-01' group by carrier;
