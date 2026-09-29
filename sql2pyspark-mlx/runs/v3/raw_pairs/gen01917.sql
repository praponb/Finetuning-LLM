select carrier, sum(cost) as total_cost, sum(sum(cost)) over (order by carrier) as cumulative_cost from shipments group by carrier order by carrier;
