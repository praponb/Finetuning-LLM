select carrier, sum(cost) total_cost, count(*) cnt, sum(sum(cost)) over (order by carrier) as cumulative_cost from shipments group by carrier order by carrier asc;
